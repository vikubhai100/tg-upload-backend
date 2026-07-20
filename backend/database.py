import os
import time
import sqlite3
from backend.config import DB_FILE_SQLITE, log

def init_db():
    os.makedirs(os.path.dirname(DB_FILE_SQLITE), exist_ok=True)
    conn = sqlite3.connect(DB_FILE_SQLITE, timeout=30.0)
    conn.execute('''CREATE TABLE IF NOT EXISTS files (
        short_id TEXT PRIMARY KEY, message_id INTEGER, filename TEXT, size INTEGER,
        content_type TEXT, channel_id INTEGER, doc_id TEXT, access_hash TEXT,
        file_reference TEXT, dc_id INTEGER, storage_type TEXT DEFAULT 'telegram', r2_key TEXT,
        last_accessed INTEGER DEFAULT 0, r2_cache_key TEXT, file_hash TEXT,
        tg_backup_msg_id INTEGER DEFAULT 0
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS used_tokens (
        sign TEXT PRIMARY KEY,
        client_ip TEXT,
        expires_at INTEGER
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS workers (
        url TEXT PRIMARY KEY,
        status TEXT DEFAULT 'healthy',
        last_checked INTEGER DEFAULT 0,
        created_at INTEGER DEFAULT 0
    )''')
    try:
        conn.execute("ALTER TABLE workers ADD COLUMN created_at INTEGER DEFAULT 0")
    except:
        pass
    # Always ensure Account B's worker is registered and marked healthy
    try:
        conn.execute("INSERT OR IGNORE INTO workers (url, status) VALUES ('https://urlkingworker.urlkings.workers.dev', 'healthy')")
        conn.execute("UPDATE workers SET status = 'healthy' WHERE url = 'https://urlkingworker.urlkings.workers.dev'")
    except Exception:
        pass

    try:
        conn.execute("ALTER TABLE files ADD COLUMN tg_backup_msg_id INTEGER DEFAULT 0")
    except:
        pass
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    conn.close()

    # Create Settings Table
    try:
        conn = sqlite3.connect(DB_FILE_SQLITE)
        conn.execute('''CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )''')
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('global_worker_mode', 'random')")
        conn.commit()
        conn.close()
    except Exception:
        pass

def get_setting(key, default=""):
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default
    except Exception:
        return default
    finally:
        conn.close()

def set_setting(key, value):
    conn = get_db_connection()
    try:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def get_db_connection():
    c = sqlite3.connect(DB_FILE_SQLITE, check_same_thread=False, timeout=30.0)
    c.row_factory = sqlite3.Row
    return c

def save_file_entry(short_id, data):
    for attempt in range(10):
        try:
            conn = get_db_connection()
            existing = conn.execute("SELECT r2_cache_key, tg_backup_msg_id FROM files WHERE short_id = ?", (short_id,)).fetchone()
            last_acc = int(time.time())
            cache_key = data.get("r2_cache_key") or (existing["r2_cache_key"] if existing else None)
            file_hash = data.get("file_hash")
            backup_msg_id = data.get("tg_backup_msg_id") or (existing["tg_backup_msg_id"] if existing else 0)

            conn.execute('''REPLACE INTO files (short_id, message_id, filename, size, content_type, channel_id, doc_id, access_hash, file_reference, dc_id, storage_type, r2_key, last_accessed, r2_cache_key, file_hash, tg_backup_msg_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (short_id, data.get("message_id", 0), data.get("filename"), data.get("size", 0),
                 data.get("content_type"), data.get("channel_id", 0), str(data.get("doc_id", "0")),
                 str(data.get("access_hash", "0")), str(data.get("file_reference", "0")),
                 data.get("dc_id", 0), data.get("storage_type", "telegram"), data.get("r2_key"), last_acc, cache_key, file_hash, backup_msg_id))
            conn.commit()
            conn.close()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.5)
                continue
            raise

def get_file_entry(short_id):
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM files WHERE short_id = ?", (short_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

# ============================================================
# 🔄 R2 CLEANUP & CACHE SWEEPER BACKGROUND LOOPS
# ============================================================
import boto3
import asyncio
from datetime import datetime, timezone, timedelta
from botocore.config import Config
from backend.config import R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET_NAME

db_r2_client = boto3.client(
    service_name='s3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

def execute_r2_cleanup():
    try:
        conn = get_db_connection()
        duplicates_hash = conn.execute('''
            SELECT file_hash, COUNT(*) as c FROM files 
            WHERE storage_type = 'r2' AND file_hash IS NOT NULL AND file_hash != '' AND r2_key IS NOT NULL
            GROUP BY file_hash HAVING c > 1
        ''').fetchall()

        for dup in duplicates_hash:
            f_hash = dup["file_hash"]
            rows = conn.execute("SELECT short_id, r2_key FROM files WHERE file_hash = ? AND storage_type = 'r2' ORDER BY rowid ASC", (f_hash,)).fetchall()
            if len(rows) > 1:
                master_r2_key = rows[0]["r2_key"]
                for i in range(1, len(rows)):
                    row_key = rows[i]["r2_key"]
                    conn.execute("UPDATE files SET r2_key = ? WHERE short_id = ?", (master_r2_key, rows[i]["short_id"]))
                    if row_key and row_key != master_r2_key:
                        still_used = conn.execute("SELECT COUNT(*) FROM files WHERE r2_key = ? AND short_id != ?", (row_key, rows[i]["short_id"])).fetchone()[0]
                        if still_used == 0:
                            try: db_r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=row_key)
                            except: pass
        conn.commit(); conn.close()
        log("R2 deduplication completed safely")
    except Exception as e:
        log(f"R2 cleanup error: {str(e)}")

def execute_cache_sweeper():
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(hours=24)
        paginator = db_r2_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix='cache_')
        conn = get_db_connection()

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['LastModified'] < cutoff_date:
                        try:
                            db_r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=obj['Key'])
                            conn.execute("UPDATE files SET r2_cache_key = NULL WHERE r2_cache_key = ?", (obj['Key'],))
                        except: pass

        current_time = int(time.time())
        conn.execute("DELETE FROM used_tokens WHERE expires_at < ?", (current_time,))
        conn.commit(); conn.close()
    except Exception: pass

async def r2_deduplication_loop():
    await asyncio.sleep(60) 
    while True:
        await asyncio.to_thread(execute_r2_cleanup)
        await asyncio.sleep(86400) 

async def cache_cleanup_loop():
    await asyncio.sleep(120) 
    while True:
        await asyncio.to_thread(execute_cache_sweeper)
        await asyncio.sleep(12 * 3600)
