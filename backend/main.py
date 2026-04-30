import os
import uuid
import tempfile
import asyncio
import time
import sqlite3
import threading
import math
import boto3
from botocore.config import Config
from pathlib import Path
from urllib.parse import quote
from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, Response, RedirectResponse
from fastapi.staticfiles import StaticFiles
import sys
import aiohttp
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import InputFileBig
from telethon.tl.functions.upload import SaveBigFilePartRequest

# ============================================================
# ☁️ CLOUDFLARE R2 CONFIG
# ============================================================
R2_ENDPOINT = "https://c756225d2d945ebc6c51149e7a1e3cfe.r2.cloudflarestorage.com"
R2_ACCESS_KEY = "6725033f7581ed01c53a5b4411dc0614"
R2_SECRET_KEY = "21295882807a0d4940dc9330e146795043b6c69ce83520f04b0be5a49262d28f"
R2_BUCKET_NAME = "urlking"

r2_client = boto3.client(
    service_name='s3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

LOG_FILE = "/tmp/telestore.log"
sys.stdout = sys.stderr

def log(msg):
    import datetime
    line = f"{datetime.datetime.now().strftime('%H:%M:%S')} | {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except:
        pass

def format_size(size_bytes):
    if size_bytes == 0: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

def get_client_ip(request: Request):
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "Unknown-IP"

app = FastAPI(title="TeleStore API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BOT_TOKEN        = os.getenv("BOT_TOKEN", "")
API_ID           = int(os.getenv("API_ID", "0"))
API_HASH         = os.getenv("API_HASH", "")
CHANNEL_ID       = int(os.getenv("CHANNEL_ID", "0"))
BASE_URL         = os.getenv("BASE_URL", "http://127.0.0.1:9500")
SESSION_STR      = os.getenv("SESSION_STRING", "")
DB_FILE_SQLITE   = "/app/data/files.db"
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "super_secret_key_123")

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

# ============================================================
# DATABASE — Hybrid Support
# ============================================================
_db_lock = threading.Lock()

def init_db():
    os.makedirs(os.path.dirname(DB_FILE_SQLITE), exist_ok=True)
    conn = sqlite3.connect(DB_FILE_SQLITE)
    conn.execute('''CREATE TABLE IF NOT EXISTS files (
        short_id    TEXT PRIMARY KEY,
        message_id  INTEGER,
        filename    TEXT,
        size        INTEGER,
        content_type TEXT,
        channel_id  INTEGER,
        doc_id      TEXT,
        access_hash TEXT,
        file_reference TEXT,
        dc_id       INTEGER,
        storage_type TEXT DEFAULT 'telegram',
        r2_key      TEXT
    )''')

    try:
        conn.execute("ALTER TABLE files ADD COLUMN storage_type TEXT DEFAULT 'telegram'")
        conn.execute("ALTER TABLE files ADD COLUMN r2_key TEXT")
    except:
        pass

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.commit()
    conn.close()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE_SQLITE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def get_file_entry(short_id):
    with _db_lock:
        conn = get_db_connection()
        row = conn.execute("SELECT * FROM files WHERE short_id = ?", (short_id,)).fetchone()
        conn.close()
    return dict(row) if row else None

def save_file_entry(short_id, data):
    with _db_lock:
        conn = get_db_connection()
        conn.execute('''REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (short_id, data.get("message_id"), data.get("filename"), data.get("size"),
             data.get("content_type"), data.get("channel_id"), str(data.get("doc_id")),
             str(data.get("access_hash")), str(data.get("file_reference")), data.get("dc_id"),
             data.get("storage_type", "telegram"), data.get("r2_key")))
        conn.commit()
        conn.close()

def delete_file_entry(short_id):
    with _db_lock:
        conn = get_db_connection()
        conn.execute("DELETE FROM files WHERE short_id = ?", (short_id,))
        conn.commit()
        conn.close()

@app.on_event("startup")
async def startup_event():
    init_db()
    try:
        client = await get_client()
        await client.get_dialogs()
        log("✅ Telegram connected and channels cached!")
    except Exception as e:
        log(f"⚠️ Telegram connect failed: {e}")

# ============================================================
# ⬇️ HYBRID DOWNLOAD ENGINE
# ============================================================
@app.get("/download/{short_id}")
async def download_file(request: Request, short_id: str):
    client_ip = get_client_ip(request)
    entry = get_file_entry(short_id)
    if not entry:
        raise HTTPException(status_code=404, detail="File not found")

    if entry.get("storage_type") == "r2":
        try:
            log(f"🚀 R2 REDIRECT | {entry['filename']} | Client: {client_ip}")
            presigned_url = r2_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': R2_BUCKET_NAME,
                    'Key': entry["r2_key"],
                    'ResponseContentDisposition': f"attachment; filename=\"{entry['filename']}\""
                },
                ExpiresIn=7200
            )
            return RedirectResponse(url=presigned_url)
        except Exception as e:
            log(f"❌ R2 URL Error: {e}")
            raise HTTPException(status_code=500, detail="R2 Link Generation Failed")

    # TELEGRAM STREAMING Logic
    file_size = int(entry["size"])
    filename_raw = entry["filename"]
    content_type = entry["content_type"] or "application/octet-stream"
    range_header = request.headers.get("Range")
    start_byte, end_byte = 0, file_size - 1

    if range_header:
        try:
            range_str = range_header.replace("bytes=", "").split("-")
            start_byte = int(range_str[0]) if range_str[0] else 0
            if len(range_str) > 1 and range_str[1]: end_byte = int(range_str[1])
        except: pass

    content_length = end_byte - start_byte + 1
    log(f"⬇️ TG STREAM START | {filename_raw} | Client: {client_ip}")

    try:
        client = await get_client()
        message = await client.get_messages(entry["channel_id"], ids=entry["message_id"])
        if not message or not message.document:
            raise HTTPException(status_code=404, detail="TG File Deleted")

        document = message.document

        async def stream_direct():
            chunk_size = 4 * 1024 * 1024
            bot_dc = getattr(client.session, 'dc_id', 0)
            file_dc = getattr(document, 'dc_id', 0)
            max_pipes = 16 if (bot_dc == file_dc or bot_dc == 0) else 8

            start_time = time.time()
            sent_bytes = 0
            last_log_time = start_time

            async def download_exact_chunk(off, length):
                async def fetch_data():
                    buf = b""
                    async for c in client.iter_download(document, offset=off, request_size=4*1024*1024):
                        buf += c
                        if len(buf) >= length: return buf[:length]
                    return buf
                return await asyncio.wait_for(fetch_data(), timeout=60.0)

            try:
                curr_off = start_byte
                pending = []
                while curr_off <= end_byte or pending:
                    if await request.is_disconnected(): break
                    while len(pending) < max_pipes and curr_off <= end_byte:
                        length = min(chunk_size, end_byte - curr_off + 1)
                        pending.append(asyncio.create_task(download_exact_chunk(curr_off, length)))
                        curr_off += length

                    if pending:
                        chunk_data = await pending.pop(0)
                        step = 512 * 1024
                        for i in range(0, len(chunk_data), step):
                            if await request.is_disconnected(): break
                            yield chunk_data[i:i+step]
                            sent_bytes += len(chunk_data[i:i+step])
                            if time.time() - last_log_time >= 5.0:
                                log(f"📡 STREAMING | {filename_raw} | {format_size(sent_bytes)}/{format_size(content_length)}")
                                last_log_time = time.time()
            except Exception as e: log(f"❌ Stream Error: {e}")

        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename_raw)}",
            "Content-Type": content_type,
            "Content-Length": str(content_length),
            "Accept-Ranges": "bytes",
            "X-Accel-Buffering": "no"
        }
        if range_header: headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{file_size}"
        return StreamingResponse(stream_direct(), status_code=206 if range_header else 200, headers=headers)
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# 🚀 REGISTER R2 FILE (Used by Node.js Finalize)
# ============================================================
@app.post("/api/file/register_r2")
async def register_r2(key: str, data: dict = Body(...)):
    if key != INTERNAL_API_KEY: raise HTTPException(status_code=403)
    save_file_entry(data["short_id"], {
        "filename": data["filename"],
        "size": data["size"],
        "storage_type": "r2",
        "r2_key": data["r2_key"],
        "content_type": "application/octet-stream",
        "message_id": 0, "channel_id": 0, "doc_id": "0", 
        "access_hash": "0", "file_reference": "0", "dc_id": 0
    })
    log(f"✅ R2 REGISTERED | {data['short_id']} | {data['filename']}")
    return {"status": "OK"}

# ============================================================
# 🗑️ SMART DELETE (Physical Storage Cleanup)
# ============================================================
@app.get("/api/file/delete")
async def mock_delete(key: str, file_code: str):
    verify_key(key)
    
    # 1. DB se details nikaalo
    entry = get_file_entry(file_code)
    
    if entry:
        # 2. Agar file R2 par hai, toh Cloudflare se mitao
        if entry.get("storage_type") == "r2" and entry.get("r2_key"):
            try:
                r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=entry["r2_key"])
                log(f"🗑️ R2 PHYSICAL DELETE | {entry['r2_key']}")
            except Exception as e:
                log(f"❌ R2 Physical Delete Failed: {e}")

        # 3. DB entry udao
        delete_file_entry(file_code)
        log(f"✅ DB ENTRY DELETED | {file_code}")
        return {"status": 200, "msg": "OK"}
    
    return {"status": 404, "msg": "File Not Found"}

# ============================================================
# UTILITIES & CLIENT
# ============================================================
_client = None
async def get_client():
    global _client
    if _client and _client.is_connected(): return _client
    _client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
    await _client.start(bot_token=BOT_TOKEN)
    return _client

def verify_key(key: str):
    if key != INTERNAL_API_KEY: raise HTTPException(status_code=403)

@app.get("/api/file/rename")
async def mock_rename(key: str, file_code: str, name: str):
    verify_key(key)
    entry = get_file_entry(file_code)
    if entry:
        entry["filename"] = name
        save_file_entry(file_code, entry)
        return {"status": 200, "msg": "OK"}
    return {"status": 404, "msg": "Not Found"}

@app.get("/api/index_forwarded")
async def index_forwarded(key: str, message_id: int, filename: str):
    verify_key(key)
    client = await get_client()
    message = await client.get_messages(CHANNEL_ID, ids=message_id)
    if not message or not message.document: return {"error": "Not Found"}
    short_id = str(uuid.uuid4())[:8]
    save_file_entry(short_id, {
        "message_id": message.id, "filename": filename, "size": message.document.size,
        "content_type": message.file.mime_type, "channel_id": CHANNEL_ID,
        "doc_id": message.document.id, "access_hash": message.document.access_hash,
        "file_reference": message.document.file_reference.hex(), "dc_id": message.document.dc_id,
        "storage_type": "telegram"
    })
    return [{"file_code": short_id, "file_status": "OK"}]
