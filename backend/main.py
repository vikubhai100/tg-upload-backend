import os
import uuid
import tempfile
import asyncio
import time
import sqlite3
import threading
import math
import boto3
import sys
import random
import string
import aiohttp
import hashlib
import hmac      
import aiofiles
import concurrent.futures
import subprocess 
import json       
import base64     
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote
from botocore.config import Config
from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Body, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, Response, RedirectResponse
from fastapi.staticfiles import StaticFiles
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
STATE_FILE = "/tmp/scan_progress.json"

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
    import datetime as dt
    line = f"{dt.datetime.now().strftime('%H:%M:%S')} | {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f: f.write(line + "\n")
    except: pass

def format_size(size_bytes):
    if size_bytes == 0: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0: return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

def safeFile(name):
    import re
    return re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', (name or 'file')).strip() or 'file'

def get_client_ip(request: Request):
    cf = request.headers.get("CF-Connecting-IP")
    if cf: return cf.strip()
    fwd = request.headers.get("X-Forwarded-For")
    return fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "Unknown")

def check_r2_file_exists(key):
    try:
        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
        return 'exists'
    except Exception as e:
        err_str = str(e).lower()
        if 'nosuchkey' in err_str or '404' in err_str or 'not found' in err_str:
            return 'not_found'
        log(f"R2 head_object error for {key}: {str(e)[:120]}")
        return 'error'

app = FastAPI(title="URLKING Hybrid Storage")

_light_executor = concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="light_db")
_heavy_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="heavy_io")

async def db_thread(fn, *args, timeout=10.0):
    try:
        return await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(_light_executor, fn, *args),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        log(f"DB operation timed out: {fn.__name__}")
        raise HTTPException(status_code=504, detail="Database operation timed out")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# 🛡️ THE IRON WALL: FULL BLOCK FOR GOOGLE BOTS & SCRAPERS
# ============================================================
@app.middleware("http")
async def bot_guard_middleware(request: Request, call_next):
    if request.url.path.startswith("/api/"):
        return await call_next(request)

    ua = request.headers.get("user-agent", "").lower()
    sec_ch_ua = request.headers.get("sec-ch-ua", "").lower()

    if any(bot in ua for bot in ["googlebot", "google", "safebrowsing", "mediapartners", "adsbot", "bingbot", "yandex", "slurp"]):
        return HTMLResponse(content="<h1>404 Not Found</h1>", status_code=404)

    if any(b in ua for b in ["python", "curl", "wget", "httpie", "postman", "crawler", "spider", "telegram", "axios", "node-fetch", "libwww"]):
        return JSONResponse(status_code=403, content={"error": "Bot Access Denied"})

    if "headless" in sec_ch_ua or any(h in ua for h in ["headlesschrome", "puppeteer", "playwright", "selenium"]):
        return JSONResponse(status_code=403, content={"error": "Headless Engine Detected"})

    return await call_next(request)

# Configuration
BOT_TOKEN        = os.getenv("BOT_TOKEN", "")
API_ID           = int(os.getenv("API_ID", "0"))
API_HASH         = os.getenv("API_HASH", "")
CHANNEL_ID       = int(os.getenv("CHANNEL_ID", "0"))
BASE_URL         = os.getenv("BASE_URL", "https://heroxclash.pro")
SESSION_STR      = os.getenv("SESSION_STRING", "")
DB_FILE_SQLITE   = "/app/data/files.db"
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "super_secret_key_123")

def calculate_hash(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192 * 1024): 
            hasher.update(chunk)
    return hasher.hexdigest()

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_file = FRONTEND_DIR / "index.html"
    if index_file.exists():
        return index_file.read_text(encoding="utf-8")
    index_alt = Path(__file__).resolve().parent / "index.html"
    if index_alt.exists():
        return index_alt.read_text(encoding="utf-8")
    return "<h2>Frontend Not Found (Check your /frontend folder)</h2>"

if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")
elif (Path(__file__).resolve().parent / "static").exists():
    app.mount("/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "static")), name="static")

# ============================================================
# 💾 DB CONNECTION & HANDLERS 
# ============================================================
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
        last_checked INTEGER DEFAULT 0
    )''')
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM workers")
        if cur.fetchone()[0] == 0:
            default_workers = [
                "https://d1.urlking.workers.dev",
                "https://download.urlking.workers.dev"
            ]
            for w in default_workers:
                cur.execute("INSERT OR IGNORE INTO workers (url, status) VALUES (?, 'healthy')", (w,))
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE files ADD COLUMN tg_backup_msg_id INTEGER DEFAULT 0")
    except:
        pass
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    conn.close()

def get_db_connection():
    c = sqlite3.connect(DB_FILE_SQLITE, check_same_thread=False, timeout=30.0)
    c.row_factory = sqlite3.Row
    return c

def save_file_entry(short_id, data):
    for _ in range(5):  
        try:
            conn = get_db_connection()
            existing = conn.execute("SELECT last_accessed, r2_cache_key, tg_backup_msg_id FROM files WHERE short_id = ?", (short_id,)).fetchone()
            last_acc = existing["last_accessed"] if existing else int(time.time())
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
                            try: r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=row_key)
                            except: pass
        conn.commit(); conn.close()
        log("R2 deduplication completed safely")
    except Exception as e:
        log(f"R2 cleanup error: {str(e)}")

def execute_cache_sweeper():
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(hours=24)
        paginator = r2_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix='cache_')
        conn = get_db_connection()

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['LastModified'] < cutoff_date:
                        try:
                            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=obj['Key'])
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

# ============================================================
# ⛅ CLOUDFLARE WORKERS AUTODEPLOY & ROTATION SYSTEM
# ============================================================
CLOUDFLARE_TOKEN = os.getenv("CLOUDFLARE_TOKEN", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "c756225d2d945ebc6c51149e7a1e3cfe")

UNIFIED_WORKER_JS = """const SECURE_SECRET = "URLKING_ANTI_SHARE_SECRET_2110";

async function verifySignature(token, signature) {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(SECURE_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"]
  );
  const sigBuffer = new Uint8Array(
    signature.match(/.{1,2}/g).map(byte => parseInt(byte, 16))
  );
  return await crypto.subtle.verify(
    "HMAC",
    key,
    sigBuffer,
    encoder.encode(token)
  );
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (path === "/health") {
      return new Response("OK", { status: 200 });
    }
    const userAgent = (request.headers.get("user-agent") || "").toLowerCase();
    const bannedCrawlers = [
      'googlebot', 'mediapartners-google', 'adsbot-google', 'bingbot', 'yandexbot', 
      'baiduspider', 'twitterbot', 'facebookexternalhit', 'google-publisher-plugin',
      'lighthouse', 'chrome-lighthouse', 'duckduckbot', 'slurp', 'ia_archiver'
    ];
    if (bannedCrawlers.some(bot => userAgent.includes(bot))) {
      return new Response("Access Denied.", { status: 403 });
    }
    if (path === "/" || path === "" || path !== "/d") {
      return new Response(`
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Nothing Here</title>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                    background-color: #ffffff;
                    color: #000000;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    justify-content: center;
                    height: 100vh;
                    margin: 0;
                    text-align: center;
                }
                .container { max-width: 600px; padding: 20px; }
                h1 { font-size: 32px; font-weight: 600; margin: 0 0 10px 0; }
                p { font-size: 16px; color: #333333; margin: 0 0 5px 0; }
                .footer { position: absolute; bottom: 20px; right: 30px; font-size: 12px; color: #666666; }
            </style>
        </head>
        <body>
            <div class="container">
                <svg width="280" height="280" viewBox="0 0 280 280" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <circle cx="140" cy="140" r="80" fill="#E0F2FE"/>
                    <path d="M110 115H170V155H110V115Z" fill="#0284C7"/>
                    <rect x="115" y="120" width="8" height="8" rx="4" fill="#F59E0B"/>
                    <rect x="127" y="120" width="8" height="8" rx="4" fill="#F59E0B"/>
                    <rect x="139" y="120" width="8" height="8" rx="4" fill="#F59E0B"/>
                    <path d="M125 145C125 140 155 140 155 145" stroke="white" stroke-width="3" stroke-linecap="round"/>
                    <circle cx="140" cy="140" r="120" stroke="#E2E8F0" stroke-width="1" stroke-dasharray="4 4"/>
                </svg>
                <h1>There is nothing here yet</h1>
                <p>If you expect something to be here, it may take some time.</p>
                <p>Please check back again later.</p>
            </div>
            <div class="footer">Powered by Cloudflare</div>
        </body>
        </html>
      `, { status: 200, headers: { "Content-Type": "text/html" } });
    }
    const token = url.searchParams.get("id");
    const signature = url.searchParams.get("sig");
    if (!token || !signature) {
      return new Response("Access Denied: Missing parameters.", { status: 400 });
    }
    const isValid = await verifySignature(token, signature);
    if (!isValid) {
      return new Response("Access Denied: Invalid signature token.", { status: 403 });
    }
    let payload;
    try {
      const decodedStr = atob(token.replace(/-/g, "+").replace(/_/g, "/"));
      payload = JSON.parse(decodedStr);
    } catch (e) {
      return new Response("Access Denied: Corrupted payload.", { status: 400 });
    }
    const { k: fileKey, n: filename, e: exp, i: authorizedIp } = payload;
    const currentTime = Math.floor(Date.now() / 1000);
    if (currentTime > parseInt(exp)) {
      return new Response("Link Expired: Download session has expired.", { status: 410 });
    }
    const clientIp = request.headers.get("cf-connecting-ip") || request.headers.get("x-real-ip") || "unknown";
    function getSubnet(ip) {
      if (ip.includes(".")) {
        return ip.split(".").slice(0, 3).join(".");
      } else if (ip.includes(":")) {
        return ip.split(":").slice(0, 3).join(":");
      }
      return ip;
    }
    if (getSubnet(clientIp) !== getSubnet(authorizedIp)) {
      return new Response("Forbidden: Restricted download network.", { status: 403 });
    }
    try {
      const object = await env.dataURLKING.get(fileKey);
      if (object === null) {
        return new Response("File Not Found.", { status: 404 });
      }
      const headers = new Headers();
      object.writeHttpMetadata(headers);
      headers.set("etag", object.httpEtag);
      headers.set("Content-Security-Policy", "default-src 'none'");
      headers.set("X-Content-Type-Options", "nosniff");
      headers.set("Content-Disposition", `attachment; filename="${filename}"`);
      return new Response(object.body, { headers });
    } catch (err) {
      return new Response("Error retrieving stream: " + err.message, { status: 500 });
    }
  }
};
"""

async def deploy_new_cloudflare_worker():
    import random
    import string
    
    random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    script_name = f"download-{random_suffix}"
    worker_url = f"https://{script_name}.urlking.workers.dev"
    
    log(f"⚡ [CLOUDFLARE API] Deploying fresh worker: {worker_url}")
    url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/workers/scripts/{script_name}"
    
    metadata = {
        "main_module": "worker.js",
        "bindings": [
            {
                "name": "dataURLKING",
                "type": "r2_bucket",
                "bucket_name": "urlking"
            }
        ]
    }
    
    data = aiohttp.FormData()
    data.add_field('metadata', json.dumps(metadata), content_type='application/json')
    data.add_field('script', UNIFIED_WORKER_JS, filename='worker.js', content_type='application/javascript+module')
    
    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_TOKEN}"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(url, data=data, headers=headers) as resp:
                result = await resp.json()
                if result.get("success"):
                    subdomain_url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/workers/scripts/{script_name}/subdomain"
                    async with session.post(subdomain_url, json={"enabled": True}, headers=headers) as s_resp:
                        s_result = await s_resp.json()
                        if s_result.get("success"):
                            log(f"✅ [CLOUDFLARE API] Subdomain enabled successfully for {script_name}")
                            conn = get_db_connection()
                            conn.execute("INSERT OR REPLACE INTO workers (url, status) VALUES (?, 'healthy')", (worker_url,))
                            conn.commit()
                            conn.close()
                            return worker_url
                log(f"❌ [CLOUDFLARE API] Failed to deploy worker: {result}")
    except Exception as e:
        log(f"❌ [CLOUDFLARE API] Exception during deployment: {str(e)}")
    return None

async def check_worker_health(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(f"{url}/health", timeout=2) as resp:
                return resp.status == 200
    except Exception:
        return False

async def get_active_worker():
    conn = get_db_connection()
    rows = conn.execute("SELECT url FROM workers WHERE status = 'healthy'").fetchall()
    conn.close()
    
    workers = [r["url"] for r in rows]
    import random
    random.shuffle(workers)
    
    for w in workers:
        if await check_worker_health(w):
            return w
        else:
            conn = get_db_connection()
            conn.execute("UPDATE workers SET status = 'flagged' WHERE url = ?", (w,))
            conn.commit()
            conn.close()
            log(f"🚨 [ROTATOR] Flagged worker removed from active rotation: {w}")
            
    # Auto-deploy a fresh worker if all current ones are flagged/blocked
    new_worker = await deploy_new_cloudflare_worker()
    if new_worker:
        return new_worker
        
    return "https://download.urlking.workers.dev"

# ============================================================
# 🔥 OPAQUE TOKEN REDIRECT
# ============================================================
async def redirect_to_r2(r2_key, filename, content_type, client_ip, log_tag="REDIRECT"):
    try:
        CUSTOM_DOMAIN = await get_active_worker()
        SECURE_SECRET = "URLKING_ANTI_SHARE_SECRET_2110"
        exp = int(time.time()) + 300 

        safe_name = safeFile(filename)
        payload_data = {"k": r2_key, "n": safe_name, "e": exp, "i": client_ip}
        payload_json = json.dumps(payload_data)
        token = base64.urlsafe_b64encode(payload_json.encode('utf-8')).decode('utf-8').rstrip('=')

        signature = hmac.new(
            SECURE_SECRET.encode('utf-8'),
            token.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        url = f"{CUSTOM_DOMAIN}/d?id={token}&sig={signature}"
        log(f"🚀 R2 {log_tag} | HIDDEN: {safe_name} | IP: {client_ip} | Worker: {CUSTOM_DOMAIN}")

        return RedirectResponse(url=url, headers={"X-Robots-Tag": "noindex, nofollow"})
    except Exception as e: 
        log(f"⚠️ Redirect Error: {str(e)}")
        raise HTTPException(status_code=500)

_active_dl = {}

async def bg_fetch_and_cache(short_id, entry):
    tmp_path = f"/tmp/dl_{short_id}.bin"
    file_size = int(entry["size"])
    current_offset = 0
    mode = "wb"

    try:
        client = await get_client()
        while current_offset < file_size:
            try:
                message = await client.get_messages(entry["channel_id"], ids=entry["message_id"])
                async with aiofiles.open(tmp_path, mode) as f_out:
                    async for chunk in client.iter_download(message.document, offset=current_offset, request_size=1024*1024):
                        await f_out.write(chunk)
                        await f_out.flush()
                        current_offset += len(chunk)
                        _active_dl[short_id]["dl_bytes"] = current_offset
                        await asyncio.sleep(0.01)
            except Exception:
                mode = "ab"
                await asyncio.sleep(2)

        _active_dl[short_id]["done"] = True

        # 🔥 STRICT 100MB CHECK: Only cache/restore to R2 if > 100MB
        if file_size > 100 * 1024 * 1024:
            r2_cache_key = f"cache_{short_id}_{uuid.uuid4().hex[:6]}"
            def s3_up():
                r2_client.upload_file(tmp_path, R2_BUCKET_NAME, r2_cache_key, ExtraArgs={'ContentType': entry["content_type"] or "application/octet-stream"})
            await asyncio.to_thread(s3_up)

            def update_cache_db():
                conn = get_db_connection()
                conn.execute("UPDATE files SET r2_cache_key = ? WHERE short_id = ?", (r2_cache_key, short_id))
                doc_id = entry.get("doc_id")
                if doc_id:
                    conn.execute("UPDATE files SET r2_cache_key = ? WHERE doc_id = ? AND r2_cache_key IS NULL", (r2_cache_key, doc_id))
                conn.commit()
                conn.close()

            await asyncio.to_thread(update_cache_db)

            entry_check = await asyncio.to_thread(get_file_entry, short_id)
            if entry_check and not entry_check.get("r2_key"):
                try:
                    restore_r2_key = f"restored_{short_id}_{uuid.uuid4().hex[:6]}"
                    def restore_to_r2():
                        r2_client.upload_file(tmp_path, R2_BUCKET_NAME, restore_r2_key, ExtraArgs={'ContentType': entry["content_type"] or "application/octet-stream"})
                    await asyncio.to_thread(restore_to_r2)

                    def update_restore_db():
                        conn = get_db_connection()
                        try:
                            conn.execute("UPDATE files SET r2_key = ?, storage_type = 'r2' WHERE short_id = ?", (restore_r2_key, short_id))
                            conn.commit()
                        finally:
                            conn.close()
                    await asyncio.to_thread(update_restore_db)
                    log(f"♻️ Auto-restored to R2: {short_id} → {restore_r2_key}")
                except Exception as restore_err:
                    log(f"⚠️ R2 restore failed for {short_id}: {str(restore_err)}")
        else:
            log(f"⚡ File {short_id} is <100MB ({file_size} bytes). Finished stream, skipping R2 Cache.")

    except Exception:
        if short_id in _active_dl: _active_dl[short_id]["err"] = True
    finally:
        await asyncio.sleep(1800)
        if short_id in _active_dl: _active_dl.pop(short_id, None)
        try: os.remove(tmp_path)
        except: pass

# ============================================================
# 📥 DOWNLOAD ENDPOINT
# ============================================================
@app.get("/download/{short_id}")
async def download_handle(request: Request, short_id: str):
    ua = request.headers.get("user-agent", "").lower()
    if any(bot in ua for bot in ["googlebot", "google", "safebrowsing", "bingbot"]):
        return HTMLResponse(content="<h1>404 Not Found</h1>", status_code=404)

    client_ip = get_client_ip(request)

    def update_last_accessed():
        conn = get_db_connection()
        try:
            conn.execute("UPDATE files SET last_accessed = ? WHERE short_id = ?", (int(time.time()), short_id))
            conn.commit()
        finally: 
            conn.close()

    await asyncio.to_thread(update_last_accessed)
    entry = await asyncio.to_thread(get_file_entry, short_id)

    if not entry:
        raise HTTPException(status_code=404, detail="File Not Found")

    file_size = int(entry.get("size", 0))

    # 🔥 LAZY CLEANUP: Agar purani file R2 par hai aur wo 100MB se chhoti hai, toh R2 se uda do!
    if entry.get("storage_type") == "r2" and file_size <= 100 * 1024 * 1024:
        log(f"🧹 Lazy Cleanup Triggered: {short_id} (<100MB) is marked as R2. Moving to Telegram stream.")
        old_r2_key = entry.get("r2_key")
        entry["storage_type"] = "telegram"
        entry["r2_key"] = None

        def fix_small_file_db():
            conn = get_db_connection()
            try:
                conn.execute("UPDATE files SET storage_type = 'telegram', r2_key = NULL WHERE short_id = ?", (short_id,))
                conn.commit()
            finally:
                conn.close()
            if old_r2_key:
                try: r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=old_r2_key)
                except: pass
                
        # Background me database aur R2 delete karo
        asyncio.create_task(asyncio.to_thread(fix_small_file_db))

    if entry.get("storage_type") == "r2" and entry.get("r2_key"):
        r2_key = entry["r2_key"]
        r2_status = await asyncio.to_thread(check_r2_file_exists, r2_key)

        if r2_status == 'exists' or r2_status == 'error':
            return await redirect_to_r2(r2_key, entry["filename"], entry.get("content_type"), client_ip, "PERMANENT")

        log(f"R2 key dead for {short_id}, attempting self-heal...")
        healed_key = None
        filename = entry.get("filename", "")
        file_hash = entry.get("file_hash")

        def find_restored_key():
            try:
                prefix = f"restored_{short_id}_"
                resp = r2_client.list_objects_v2(Bucket=R2_BUCKET_NAME, Prefix=prefix, MaxKeys=5)
                if 'Contents' in resp:
                    for obj in resp['Contents']:
                        k = obj['Key']
                        if k.startswith(prefix) and obj['Size'] > 0: return k
            except: pass
            return None

        restored = await asyncio.to_thread(find_restored_key)
        if restored: healed_key = restored

        if not healed_key and file_hash:
            def find_by_hash():
                c = get_db_connection()
                try:
                    r = c.execute("SELECT r2_key FROM files WHERE file_hash = ? AND r2_key IS NOT NULL AND short_id != ?", (file_hash, short_id)).fetchone()
                    return r["r2_key"] if r else None
                finally: c.close()
            candidate = await asyncio.to_thread(find_by_hash)
            if candidate:
                if await asyncio.to_thread(check_r2_file_exists, candidate) == 'exists':
                    healed_key = candidate

        if not healed_key:
            def find_by_shortid_prefix():
                try:
                    prefix = f"{short_id}_"
                    resp = r2_client.list_objects_v2(Bucket=R2_BUCKET_NAME, Prefix=prefix, MaxKeys=5)
                    if 'Contents' in resp:
                        for obj in resp['Contents']:
                            k = obj['Key']
                            if k.startswith(prefix) and obj['Size'] > 0: return k
                except: pass
                return None
            found = await asyncio.to_thread(find_by_shortid_prefix)
            if found: healed_key = found

        if not healed_key and filename:
            def search_r2():
                try:
                    for page in r2_client.get_paginator('list_objects_v2').paginate(Bucket=R2_BUCKET_NAME):
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                k = obj['Key']
                                if k.startswith('cache_') or k == r2_key: continue
                                if filename in k or k.endswith(safeFile(filename)):
                                    try:
                                        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=k)
                                        return k
                                    except: pass
                    return None
                except: return None
            found = await asyncio.to_thread(search_r2)
            if found: healed_key = found

        if healed_key:
            def heal_db():
                c = get_db_connection()
                try:
                    c.execute("UPDATE files SET r2_key = ? WHERE short_id = ?", (healed_key, short_id))
                    c.execute("UPDATE files SET r2_key = ? WHERE r2_key = ? AND storage_type = 'r2'", (healed_key, r2_key))
                    c.commit()
                finally: c.close()
            await asyncio.to_thread(heal_db)
            return await redirect_to_r2(healed_key, entry["filename"], entry.get("content_type"), client_ip, "HEALED")

        backup_msg_id = entry.get("tg_backup_msg_id") or entry.get("message_id")
        if backup_msg_id and int(backup_msg_id) > 0:
            def fallback_db_fix():
                conn = get_db_connection()
                conn.execute("UPDATE files SET r2_key = NULL, storage_type = 'telegram' WHERE short_id = ?", (short_id,))
                conn.commit(); conn.close()
            await asyncio.to_thread(fallback_db_fix)
            entry["storage_type"] = "telegram"
            entry["r2_key"] = None
            entry["message_id"] = int(backup_msg_id)
        else:
            return HTMLResponse(content="<div style='font-family:sans-serif; text-align:center; margin-top:50px; color:#991b1b;'><h2>File Deleted</h2></div>", status_code=404)

    if entry.get("r2_cache_key"):
        # 🔥 LAZY CACHE CLEANUP: 100MB se chhoti file cache me se delete karo!
        if file_size <= 100 * 1024 * 1024:
            old_cache = entry.get("r2_cache_key")
            entry["r2_cache_key"] = None
            def fix_small_cache_db():
                conn = get_db_connection()
                try:
                    conn.execute("UPDATE files SET r2_cache_key = NULL WHERE short_id = ?", (short_id,))
                    conn.commit()
                finally:
                    conn.close()
                if old_cache:
                    try: r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=old_cache)
                    except: pass
            asyncio.create_task(asyncio.to_thread(fix_small_cache_db))
        else:
            cache_status = await asyncio.to_thread(check_r2_file_exists, entry["r2_cache_key"])
            if cache_status == 'exists' or cache_status == 'error':
                return await redirect_to_r2(entry["r2_cache_key"], entry["filename"], entry.get("content_type"), client_ip, "CACHED LINK")

            if entry.get("message_id") and int(entry.get("message_id")) > 0:
                def remove_cache_key():
                    conn = get_db_connection()
                    conn.execute("UPDATE files SET r2_cache_key = NULL WHERE short_id = ?", (short_id,))
                    conn.commit(); conn.close()
                await asyncio.to_thread(remove_cache_key)

    tmp_path = f"/tmp/dl_{short_id}.bin"
    if short_id not in _active_dl:
        _active_dl[short_id] = {"dl_bytes": 0, "done": False, "err": False}
        asyncio.create_task(bg_fetch_and_cache(short_id, entry))

    for _ in range(50):
        if os.path.exists(tmp_path) and _active_dl.get(short_id, {}).get("dl_bytes", 0) > 0: break
        await asyncio.sleep(0.2)

    if not os.path.exists(tmp_path): raise HTTPException(500, "Failed to connect to Backend")

    filename_raw = entry["filename"]
    content_type = entry["content_type"] or "application/octet-stream"
    range_header = request.headers.get("Range")
    start_byte, end_byte = 0, file_size - 1

    if range_header:
        try:
            r_str = range_header.replace("bytes=", "").split("-")
            start_byte = int(r_str[0]) if r_str[0] else 0
            if len(r_str) > 1 and r_str[1]: end_byte = int(r_str[1])
        except: pass

    content_length = end_byte - start_byte + 1

    async def temp_file_streamer():
        async with aiofiles.open(tmp_path, "rb") as f:
            await f.seek(start_byte)
            curr = start_byte
            while curr <= end_byte:
                if await request.is_disconnected(): break
                info = _active_dl.get(short_id)
                if not info: break
                target_bytes = info["dl_bytes"]

                while curr >= target_bytes:
                    info = _active_dl.get(short_id)
                    if not info or info["done"]: break
                    if info["err"]: raise RuntimeError("Backend dropped")
                    await asyncio.sleep(0.2)
                    target_bytes = _active_dl.get(short_id, {}).get("dl_bytes", 0)

                info = _active_dl.get(short_id, {})
                if info.get("done", False) and curr >= target_bytes: break
                avail = target_bytes - curr
                read_size = min(128 * 1024, end_byte - curr + 1)
                if not info.get("done", False): read_size = min(read_size, avail)

                if read_size > 0:
                    data = await f.read(read_size)
                    if not data:
                        await asyncio.sleep(0.1)
                        await f.seek(curr)
                        continue
                    yield data
                    curr += len(data)

    headers = {
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename_raw)}",
        "Content-Type": content_type,
        "Content-Length": str(content_length),
        "Accept-Ranges": "bytes",
        "X-Accel-Buffering": "no",
        "X-Robots-Tag": "noindex, nofollow",     
        "X-Content-Type-Options": "nosniff"      
    }
    if range_header: headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{file_size}"
    return StreamingResponse(temp_file_streamer(), status_code=206 if range_header else 200, headers=headers)

async def parallel_upload(client, file_path):
    size = os.path.getsize(file_path)
    name = os.path.basename(file_path)

    if size < 10*1024*1024: 
        uploaded = await client.upload_file(file_path)
        return uploaded

    CHUNK_SIZE = 512 * 1024  
    f_id = int.from_bytes(os.urandom(8), "big", signed=True)
    parts = math.ceil(size / CHUNK_SIZE)
    sem = asyncio.Semaphore(4)  
    upload_errors = []

    async def up_part(idx):
        async with sem:
            offset = idx * CHUNK_SIZE
            read_size = min(CHUNK_SIZE, size - offset)
            async with aiofiles.open(file_path, 'rb') as f:
                await f.seek(offset)
                chunk = await f.read(read_size)
            try:
                await client(SaveBigFilePartRequest(f_id, idx, parts, chunk))
            except Exception as e:
                upload_errors.append(f"Part {idx}: {str(e)}")
                raise

    await asyncio.gather(*[up_part(i) for i in range(parts)], return_exceptions=False)
    return InputFileBig(id=f_id, parts=parts, name=name)

@app.post("/api/upload")
async def api_upload(request: Request):
    try:
        form = await request.form()
        key = request.query_params.get("key") or form.get("key")
        verify_key(key)

        file_obj = None
        for k, v in form.items():
            if hasattr(v, "filename") and getattr(v, "filename", None):
                file_obj = v; break

        if not file_obj: return JSONResponse(status_code=400, content=[{"error": "No file detected"}])

        filename = file_obj.filename
        content_type = getattr(file_obj, "content_type", "application/octet-stream")
        tmp_path = f"/tmp/{uuid.uuid4().hex}.bin"  # 🔥 STRICTLY BIN

        async with aiofiles.open(tmp_path, "wb") as f:
            while chunk := await file_obj.read(2 * 1024 * 1024): await f.write(chunk)

        file_hash = await asyncio.to_thread(calculate_hash, tmp_path)

        def fetch_existing():
            conn = get_db_connection()
            try: return conn.execute("SELECT * FROM files WHERE file_hash = ?", (file_hash,)).fetchone()
            finally: conn.close()

        existing = await asyncio.to_thread(fetch_existing)

        if existing:
            os.unlink(tmp_path)
            new_id = str(uuid.uuid4())[:8]
            await asyncio.to_thread(save_file_entry, new_id, {
                "message_id": existing["message_id"], "filename": filename, "size": existing["size"],
                "content_type": content_type, "channel_id": existing["channel_id"],
                "doc_id": existing["doc_id"], "access_hash": existing["access_hash"],
                "file_reference": existing["file_reference"], "dc_id": existing["dc_id"],
                "storage_type": existing["storage_type"], "r2_key": existing["r2_key"],
                "file_hash": file_hash, "r2_cache_key": existing.get("r2_cache_key")
            })
            return JSONResponse(content=[{"file_code": new_id, "file_status": "OK"}])

        client = await get_client()
        file_size = os.path.getsize(tmp_path)

        try:
            if file_size < 10 * 1024 * 1024:
                msg = await client.send_file(CHANNEL_ID, tmp_path, force_document=True)
            else:
                uploaded_file = await parallel_upload(client, tmp_path)
                msg = await client.send_file(CHANNEL_ID, file=uploaded_file, force_document=True)
        except Exception as send_err:
            global _client
            async with _client_lock:
                try:
                    if _client: await _client.disconnect()
                except: pass
                _client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
                await _client.start(bot_token=BOT_TOKEN)
                client = _client
            if file_size < 10 * 1024 * 1024:
                msg = await client.send_file(CHANNEL_ID, tmp_path, force_document=True)
            else:
                uploaded_file = await parallel_upload(client, tmp_path)
                msg = await client.send_file(CHANNEL_ID, file=uploaded_file, force_document=True)

        # 🔥 STRICT 100MB CHECK FOR R2 UPLOAD
        storage_type = "telegram"
        r2_key = None
        
        if file_size > 100 * 1024 * 1024:
            r2_key = f"r2_{uuid.uuid4().hex[:8]}"
            def upload_to_r2():
                with open(tmp_path, 'rb') as f:
                    r2_client.upload_fileobj(f, R2_BUCKET_NAME, r2_key)
            await asyncio.to_thread(upload_to_r2)
            storage_type = "r2"

        short_id = str(uuid.uuid4())[:8]
        await asyncio.to_thread(save_file_entry, short_id, {
            "message_id": msg.id, "filename": filename, "size": file_size,
            "content_type": content_type, "channel_id": CHANNEL_ID,
            "doc_id": msg.document.id, "access_hash": msg.document.access_hash,
            "file_reference": msg.document.file_reference.hex(), "dc_id": msg.document.dc_id,
            "file_hash": file_hash, "storage_type": storage_type, "r2_key": r2_key
        })
        try: os.unlink(tmp_path)
        except: pass
        return JSONResponse(content=[{"file_code": short_id, "file_status": "OK"}])
    except Exception as e:
        try: os.unlink(tmp_path)
        except: pass
        return JSONResponse(status_code=500, content=[{"error": str(e)}])

@app.post("/api/remote_upload")
async def remote_upload(request: Request):
    try:
        data = await request.json()
        verify_key(data.get("key"))
        url = data.get("url")
        filename = data.get("filename", f"file_{int(time.time())}.bin")
        tmp_path = f"/tmp/remote_{uuid.uuid4().hex[:8]}.bin" # 🔥 STRICTLY BIN

        download_timeout = aiohttp.ClientTimeout(total=1800, connect=30, sock_read=120) 
        last_err = None
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(timeout=download_timeout) as session:
                    async with session.get(url, allow_redirects=True, ssl=False) as r:
                        if r.status != 200:
                            raise Exception(f"Remote server returned HTTP {r.status}")
                        async with aiofiles.open(tmp_path, 'wb') as f:
                            async for chunk in r.content.iter_chunked(5*1024*1024): 
                                await f.write(chunk)
                last_err = None
                break
            except Exception as dl_err:
                last_err = dl_err
                if attempt < 2:
                    await asyncio.sleep(3 * (attempt + 1))
                    try: os.unlink(tmp_path)
                    except: pass

        if last_err:
            try: os.unlink(tmp_path)
            except: pass
            return {"error": f"Remote download failed after 3 attempts: {str(last_err)}"}

        file_hash = await asyncio.to_thread(calculate_hash, tmp_path)

        def fetch_existing():
            conn = get_db_connection()
            try: return conn.execute("SELECT * FROM files WHERE file_hash = ?", (file_hash,)).fetchone()
            finally: conn.close()

        existing = await asyncio.to_thread(fetch_existing)

        if existing:
            os.unlink(tmp_path)
            new_short_id = str(uuid.uuid4())[:8]
            await asyncio.to_thread(save_file_entry, new_short_id, {
                "message_id": existing["message_id"], "filename": filename, "size": existing["size"],
                "content_type": existing["content_type"], "channel_id": existing["channel_id"],
                "doc_id": existing["doc_id"], "access_hash": existing["access_hash"],
                "file_reference": existing["file_reference"], "dc_id": existing["dc_id"],
                "storage_type": existing["storage_type"], "r2_key": existing["r2_key"],
                "file_hash": file_hash, "r2_cache_key": existing.get("r2_cache_key")
            })
            return [{"file_code": new_short_id, "file_status": "OK"}]

        client = await get_client()
        actual_size = os.path.getsize(tmp_path)

        import mimetypes
        detected_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

        try:
            if actual_size < 10 * 1024 * 1024:
                msg = await client.send_file(CHANNEL_ID, tmp_path, force_document=True)
            else:
                uploaded_file = await parallel_upload(client, tmp_path)
                msg = await client.send_file(CHANNEL_ID, file=uploaded_file, force_document=True)
        except Exception as send_err:
            global _client
            async with _client_lock:
                try:
                    if _client: await _client.disconnect()
                except: pass
                _client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
                await _client.start(bot_token=BOT_TOKEN)
                client = _client
            if actual_size < 10 * 1024 * 1024:
                msg = await client.send_file(CHANNEL_ID, tmp_path, force_document=True)
            else:
                uploaded_file = await parallel_upload(client, tmp_path)
                msg = await client.send_file(CHANNEL_ID, file=uploaded_file, force_document=True)

        # 🔥 STRICT 100MB CHECK FOR R2 UPLOAD
        storage_type = "telegram"
        r2_key = None

        if actual_size > 100 * 1024 * 1024:
            r2_key = f"r2_{uuid.uuid4().hex[:8]}"
            def upload_to_r2():
                with open(tmp_path, 'rb') as f:
                    r2_client.upload_fileobj(f, R2_BUCKET_NAME, r2_key)
            await asyncio.to_thread(upload_to_r2)
            storage_type = "r2"

        short_id = str(uuid.uuid4())[:8]

        await asyncio.to_thread(save_file_entry, short_id, {
            "message_id": msg.id, "filename": filename, "size": actual_size,
            "content_type": detected_type, "channel_id": CHANNEL_ID,
            "doc_id": msg.document.id, "access_hash": msg.document.access_hash,
            "file_reference": msg.document.file_reference.hex(), "dc_id": msg.document.dc_id,
            "file_hash": file_hash, "storage_type": storage_type, "r2_key": r2_key
        })

        try: os.unlink(tmp_path)
        except: pass
        return [{"file_code": short_id, "file_status": "OK"}]
    except Exception as e:
        try: os.unlink(tmp_path)
        except: pass
        return {"error": str(e)}

@app.post("/api/file/register_r2")
async def register_r2(key: str, background_tasks: BackgroundTasks, data: dict = Body(...)):
    verify_key(key)

    file_hash = data.get("file_hash")
    new_r2_key = data.get("r2_key")
    filename = data.get("filename")
    size = int(data.get("size", 0))

    if not new_r2_key:
        raise HTTPException(status_code=400, detail="Missing r2_key")

    def check_existing_db():
        conn = get_db_connection()
        try:
            if file_hash and str(file_hash).strip():
                ext = conn.execute("SELECT r2_key FROM files WHERE file_hash = ? AND storage_type = 'r2' AND r2_key IS NOT NULL", (file_hash,)).fetchone()
                if ext: return dict(ext)
            if filename and size > 0:
                ext = conn.execute("SELECT r2_key FROM files WHERE filename = ? AND size = ? AND storage_type = 'r2' AND r2_key IS NOT NULL", (filename, size)).fetchone()
                if ext: return dict(ext)
            return None
        finally:
            conn.close()

    existing = await asyncio.to_thread(check_existing_db)

    if existing:
        master_r2_key = existing["r2_key"]

        if master_r2_key == new_r2_key:
            await asyncio.to_thread(save_file_entry, data["short_id"], {
                "filename": filename, "size": size, "storage_type": "r2",
                "r2_key": new_r2_key, "content_type": "application/octet-stream",
                "file_hash": file_hash
            })
            return {"status": "OK", "msg": "Same key, saved directly"}

        master_status = await asyncio.to_thread(check_r2_file_exists, master_r2_key)

        if master_status == 'exists':
            await asyncio.to_thread(save_file_entry, data["short_id"], {
                "filename": filename, "size": size, "storage_type": "r2",
                "r2_key": master_r2_key, "content_type": "application/octet-stream",
                "file_hash": file_hash
            })

            def delete_redundant():
                try: r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=new_r2_key)
                except: pass
            background_tasks.add_task(delete_redundant)
            return {"status": "OK", "msg": "Duplicate handled instantly"}
        else:
            def fix_dead_master():
                conn = get_db_connection()
                try:
                    conn.execute("UPDATE files SET r2_key = ? WHERE r2_key = ? AND storage_type = 'r2'", (new_r2_key, master_r2_key))
                    conn.commit()
                finally:
                    conn.close()
            await asyncio.to_thread(fix_dead_master)

    await asyncio.to_thread(save_file_entry, data["short_id"], {
        "filename": filename, "size": size, "storage_type": "r2",
        "r2_key": new_r2_key, "content_type": "application/octet-stream",
        "file_hash": file_hash
    })
    return {"status": "OK"}

@app.get("/api/file/clone")
async def file_clone(key: str, file_code: str):
    verify_key(key)
    if not file_code or not file_code.strip():
        return JSONResponse(status_code=400, content={"status": 400, "error": "Missing file_code"})

    try:
        entry = await db_thread(get_file_entry, file_code, timeout=10.0)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": 500, "error": f"Database error: {str(e)}"})

    if not entry:
        return JSONResponse(status_code=404, content={"status": 404, "error": "File not found"})

    new_id = str(uuid.uuid4())[:8]

    try:
        clean_entry = {k: v for k, v in entry.items() if k != "short_id"}
        await db_thread(save_file_entry, new_id, clean_entry, timeout=10.0)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": 500, "error": f"Failed to save clone: {str(e)}"})
    return {"status": 200, "result": {"filecode": new_id}}

# ============================================================
# 🔥 ONE-CLICK API TO DELETE ALL <100MB FILES FROM R2
# ============================================================
@app.get("/api/clean_r2_small_files")
async def clean_r2_small_files(key: str):
    verify_key(key)
    def run_cleanup():
        conn = get_db_connection()
        # Wo saari files find karo jo 100MB se chhoti hain aur R2 pe mapped hain
        rows = conn.execute("SELECT short_id, r2_key FROM files WHERE size <= ? AND storage_type = 'r2' AND r2_key IS NOT NULL", (100 * 1024 * 1024,)).fetchall()
        count = 0
        for row in rows:
            try:
                # 1. R2 se uda do
                r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=row["r2_key"])
                # 2. Database update kar do
                conn.execute("UPDATE files SET storage_type = 'telegram', r2_key = NULL WHERE short_id = ?", (row["short_id"],))
                count += 1
            except: pass
        conn.commit()
        conn.close()
        return count

    deleted_count = await asyncio.to_thread(run_cleanup)
    return {"status": "OK", "msg": f"Deleted {deleted_count} small files (<100MB) from R2 and forced to Telegram stream."}

# ============================================================
# 🔥 FILE DELETE LOGIC
# ============================================================
@app.get("/api/file/delete")
async def file_delete(key: str, file_code: str):
    verify_key(key)
    try:
        entry = await db_thread(get_file_entry, file_code, timeout=10.0)
    except Exception as e:
        return {"status": 500, "error": str(e)}
    if entry:
        def run_delete():
            conn = get_db_connection()
            try:
                if entry.get("storage_type") == "r2" and entry.get("r2_key"):
                    count = conn.execute("SELECT COUNT(*) FROM files WHERE r2_key = ?", (entry["r2_key"],)).fetchone()[0]
                    if count <= 1:
                        try: r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=entry["r2_key"])
                        except: pass
                if entry.get("r2_cache_key"):
                    count = conn.execute("SELECT COUNT(*) FROM files WHERE r2_cache_key = ?", (entry["r2_cache_key"],)).fetchone()[0]
                    if count <= 1:
                        try: r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=entry["r2_cache_key"])
                        except: pass
                conn.execute("DELETE FROM files WHERE short_id = ?", (file_code,))
                conn.commit()
            finally:
                conn.close()
        await asyncio.to_thread(run_delete)
    return {"status": 200, "msg": "OK"}

@app.get("/api/file/rename")
async def file_rename(key: str, file_code: str, name: str):
    verify_key(key)
    safe_name = name.strip().replace("/", "_").replace("\\", "_").replace("\x00", "")
    if not safe_name: raise HTTPException(status_code=400, detail="Invalid filename")

    def run_rename():
        conn = get_db_connection()
        try:
            result = conn.execute("UPDATE files SET filename = ? WHERE short_id = ?", (safe_name, file_code))
            conn.commit()
            return result.rowcount
        finally:
            conn.close()

    try:
        changed = await db_thread(run_rename, timeout=10.0)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": 500, "error": f"Rename failed: {str(e)}"})

    if changed == 0: return JSONResponse(status_code=404, content={"status": 404, "msg": "File not found"})
    return {"status": 200, "msg": "OK", "new_name": safe_name}

@app.get("/api/file/info")
async def file_info(key: str, file_code: str):
    verify_key(key)
    try:
        entry = await db_thread(get_file_entry, file_code, timeout=10.0)
    except Exception:
        return {"result": []}
    if entry: return {"result": [{"name": entry["filename"], "size": entry["size"], "storage": entry.get("storage_type", "telegram")}]}
    return {"result": []}

@app.get("/api/index_forwarded")
async def index_forwarded(key: str, message_id: int, filename: str):
    verify_key(key)
    client = await get_client()
    message = await client.get_messages(CHANNEL_ID, ids=message_id)
    if not message or not message.document: return {"error": "Not Found"}

    doc_id_str = str(message.document.id)
    def fetch_existing():
        conn = get_db_connection()
        try: return conn.execute("SELECT * FROM files WHERE doc_id = ?", (doc_id_str,)).fetchone()
        finally: conn.close()

    try:
        existing = await db_thread(fetch_existing, timeout=10.0)
    except Exception:
        existing = None

    if existing:
        try: await client.delete_messages(CHANNEL_ID, message_id)
        except: pass
        new_id = str(uuid.uuid4())[:8]
        try:
            await db_thread(save_file_entry, new_id, {
                "message_id": existing["message_id"], "filename": filename, "size": existing["size"],
                "content_type": existing["content_type"], "channel_id": existing["channel_id"],
                "doc_id": existing["doc_id"], "access_hash": existing["access_hash"],
                "file_reference": existing["file_reference"], "dc_id": existing["dc_id"],
                "storage_type": existing["storage_type"], "r2_key": existing["r2_key"],
                "file_hash": existing.get("file_hash"), "r2_cache_key": existing.get("r2_cache_key")
            }, timeout=10.0)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Save failed: {str(e)}")
        return [{"file_code": new_id, "file_status": "OK"}]

    short_id = str(uuid.uuid4())[:8]
    try:
        await db_thread(save_file_entry, short_id, {
            "message_id": message.id, "filename": filename, "size": message.document.size,
            "content_type": message.file.mime_type, "channel_id": CHANNEL_ID,
            "doc_id": message.document.id, "access_hash": message.document.access_hash,
            "file_reference": message.document.file_reference.hex(), "dc_id": message.document.dc_id,
            "storage_type": "telegram"
        }, timeout=10.0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Save failed: {str(e)}")
    return [{"file_code": short_id, "file_status": "OK"}]

@app.post("/api/fast_index")
async def fast_index(key: str, data: dict = Body(...)):
    verify_key(key)
    message_id = data.get("message_id")
    filename = data.get("filename")
    size = data.get("size", 0)
    content_type = data.get("content_type", "application/octet-stream")
    doc_id = data.get("doc_id")
    access_hash = data.get("access_hash")
    file_reference = data.get("file_reference")
    dc_id = data.get("dc_id")

    if not message_id or not filename:
        raise HTTPException(status_code=400, detail="message_id and filename required")

    if doc_id:
        def fetch_existing():
            conn = get_db_connection()
            try: return conn.execute("SELECT * FROM files WHERE doc_id = ?", (str(doc_id),)).fetchone()
            finally: conn.close()
        try:
            existing = await db_thread(fetch_existing, timeout=10.0)
        except Exception as e:
            existing = None

        if existing:
            new_id = str(uuid.uuid4())[:8]
            try:
                await db_thread(save_file_entry, new_id, {
                    "message_id": existing["message_id"], "filename": filename, "size": existing["size"],
                    "content_type": existing["content_type"], "channel_id": existing["channel_id"],
                    "doc_id": existing["doc_id"], "access_hash": existing["access_hash"],
                    "file_reference": existing["file_reference"], "dc_id": existing["dc_id"],
                    "storage_type": existing["storage_type"], "r2_key": existing["r2_key"],
                    "file_hash": existing.get("file_hash"), "r2_cache_key": existing.get("r2_cache_key")
                }, timeout=10.0)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to save: {str(e)}")
            return [{"file_code": new_id, "file_status": "OK"}]

    short_id = str(uuid.uuid4())[:8]
    try:
        await db_thread(save_file_entry, short_id, {
            "message_id": int(message_id), "filename": filename, "size": int(size),
            "content_type": content_type, "channel_id": CHANNEL_ID,
            "doc_id": str(doc_id or "0"), "access_hash": str(access_hash or "0"),
            "file_reference": str(file_reference or "0"), "dc_id": int(dc_id or 0),
            "storage_type": "telegram"
        }, timeout=10.0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save entry: {str(e)}")
    return [{"file_code": short_id, "file_status": "OK"}]

@app.post("/api/re_index")
async def re_index(key: str, data: dict = Body(...)):
    verify_key(key)
    short_id = data.get("short_id")
    message_id = data.get("message_id")
    filename = data.get("filename")
    size = data.get("size", 0)
    content_type = data.get("content_type", "application/octet-stream")
    doc_id = data.get("doc_id")
    access_hash = data.get("access_hash")
    file_reference = data.get("file_reference")
    dc_id = data.get("dc_id")
    storage_type = data.get("storage_type", "telegram")
    r2_key = data.get("r2_key")
    file_hash = data.get("file_hash")

    if not short_id or not filename:
        raise HTTPException(status_code=400, detail="short_id and filename required")

    existing = await db_thread(get_file_entry, short_id, timeout=5.0)
    if existing:
        return {"status": "OK", "msg": "Entry already exists", "file_code": short_id}

    try:
        await db_thread(save_file_entry, short_id, {
            "message_id": int(message_id or 0), "filename": filename, "size": int(size),
            "content_type": content_type, "channel_id": CHANNEL_ID,
            "doc_id": str(doc_id or "0"), "access_hash": str(access_hash or "0"),
            "file_reference": str(file_reference or "0"), "dc_id": int(dc_id or 0),
            "storage_type": storage_type, "r2_key": r2_key, "file_hash": file_hash
        }, timeout=10.0)
        return {"status": "OK", "msg": "Entry created", "file_code": short_id}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": 500, "error": str(e)})

@app.post("/api/file/backup_to_telegram")
async def backup_to_telegram(key: str, data: dict = Body(...)):
    verify_key(key)
    file_code = data.get("file_code")
    if not file_code:
        raise HTTPException(status_code=400, detail="Missing file_code")

    entry = await asyncio.to_thread(get_file_entry, file_code)
    if not entry:
        raise HTTPException(status_code=404, detail="File not found in DB")

    if entry.get("tg_backup_msg_id") and int(entry.get("tg_backup_msg_id", 0)) > 0:
        return {"status": "OK", "msg": "Backup already exists", "backup_msg_id": entry["tg_backup_msg_id"]}

    r2_key = entry.get("r2_key")
    if not r2_key:
        return {"status": "OK", "msg": "No R2 key, nothing to backup"}

    if entry.get("message_id") and int(entry.get("message_id", 0)) > 0:
        def mark_backup():
            conn = get_db_connection()
            try:
                conn.execute("UPDATE files SET tg_backup_msg_id = message_id WHERE short_id = ?", (file_code,))
                conn.commit()
            finally:
                conn.close()
        await asyncio.to_thread(mark_backup)
        return {"status": "OK", "msg": "Original Telegram message exists, marked as backup", "backup_msg_id": entry["message_id"]}

    tmp_path = f"/tmp/backup_{file_code}_{uuid.uuid4().hex[:6]}.bin"
    try:
        def download_from_r2():
            r2_client.download_file(R2_BUCKET_NAME, r2_key, tmp_path)
        await asyncio.to_thread(download_from_r2)

        client = await get_client()
        file_size = os.path.getsize(tmp_path)

        if file_size > 10 * 1024 * 1024:
            uploaded_file = await parallel_upload(client, tmp_path)
            msg = await client.send_file(CHANNEL_ID, file=uploaded_file, force_document=True)
        else:
            msg = await client.send_file(CHANNEL_ID, tmp_path, force_document=True)

        def update_backup():
            conn = get_db_connection()
            try:
                conn.execute(
                    "UPDATE files SET tg_backup_msg_id = ?, message_id = ?, channel_id = ?, doc_id = ?, access_hash = ?, file_reference = ?, dc_id = ? WHERE short_id = ?",
                    (msg.id, msg.id, CHANNEL_ID, str(msg.document.id),
                     str(msg.document.access_hash), msg.document.file_reference.hex(),
                     msg.document.dc_id, file_code))
                conn.commit()
            finally:
                conn.close()
        await asyncio.to_thread(update_backup)

        return {"status": "OK", "msg": "Backed up to Telegram", "backup_msg_id": msg.id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backup failed: {str(e)}")
    finally:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except:
            pass

@app.get("/files")
async def list_files(key: str, page: int = 1, limit: int = 10):
    verify_key(key)
    def get_files():
        conn = get_db_connection()
        try:
            offset = (page - 1) * limit
            total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            rows = conn.execute("SELECT * FROM files ORDER BY rowid DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
            return total, rows
        finally:
            conn.close()

    total, rows = await asyncio.to_thread(get_files)
    return {
        "files": [{"short_id": r["short_id"], "filename": r["filename"], "size": format_size(r["size"]), "download_link": f"{BASE_URL}/download/{r['short_id']}"} for r in rows],
        "total": total, "page": page, "total_pages": math.ceil(total / limit) if total > 0 else 1
    }

def _repair_r2_sync():
    conn = get_db_connection()
    entries = conn.execute("SELECT short_id, r2_key, filename, size, file_hash, storage_type, message_id, tg_backup_msg_id FROM files WHERE storage_type = 'r2' AND r2_key IS NOT NULL").fetchall()
    conn.close()

    ok = 0; broken = 0; fixed = 0; unfixed = 0

    for entry in entries:
        try:
            r2_key = entry["r2_key"]
            short_id = entry["short_id"]

            try:
                r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
                ok += 1
                continue
            except Exception as e:
                err_str = str(e).lower()
                if '404' not in err_str and 'nosuchkey' not in err_str:
                    ok += 1
                    continue

            broken += 1

            fixed_key = None
            try:
                prefix = f"restored_{short_id}_"
                resp = r2_client.list_objects_v2(Bucket=R2_BUCKET_NAME, Prefix=prefix, MaxKeys=5)
                if 'Contents' in resp:
                    for obj in resp['Contents']:
                        if obj['Key'].startswith(prefix) and obj['Size'] > 0:
                            fixed_key = obj['Key']
                            break
            except:
                pass

            if fixed_key:
                c_r = get_db_connection()
                try:
                    c_r.execute("UPDATE files SET r2_key = ? WHERE short_id = ?", (fixed_key, short_id))
                    c_r.commit()
                finally:
                    c_r.close()
                fixed += 1
                continue

            try:
                prefix = f"{short_id}_"
                resp = r2_client.list_objects_v2(Bucket=R2_BUCKET_NAME, Prefix=prefix, MaxKeys=5)
                if 'Contents' in resp:
                    for obj in resp['Contents']:
                        if obj['Key'].startswith(prefix) and obj['Size'] > 0:
                            fixed_key = obj['Key']
                            break
            except:
                pass

            if fixed_key:
                c_s = get_db_connection()
                try:
                    c_s.execute("UPDATE files SET r2_key = ? WHERE short_id = ?", (fixed_key, short_id))
                    c_s.commit()
                finally:
                    c_s.close()
                fixed += 1
                continue

            file_hash = entry["file_hash"]

            if file_hash:
                c2 = get_db_connection()
                try:
                    row = c2.execute("SELECT r2_key FROM files WHERE file_hash = ? AND r2_key IS NOT NULL AND short_id != ?", (file_hash, short_id)).fetchone()
                    if row:
                        candidate = row["r2_key"]
                        try:
                            r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=candidate)
                            fixed_key = candidate
                        except:
                            pass
                finally:
                    c2.close()

            if fixed_key:
                c3 = get_db_connection()
                try:
                    c3.execute("UPDATE files SET r2_key = ? WHERE short_id = ?", (fixed_key, short_id))
                    c3.commit()
                finally:
                    c3.close()
                fixed += 1
                continue

            filename = entry["filename"]
            if filename:
                try:
                    for page in r2_client.get_paginator('list_objects_v2').paginate(Bucket=R2_BUCKET_NAME):
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                k = obj['Key']
                                if k.startswith('cache_') or k == r2_key:
                                    continue
                                if filename in k:
                                    try:
                                        r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=k)
                                        fixed_key = k
                                        break
                                    except:
                                        pass
                        if fixed_key:
                            break
                except Exception:
                    pass

            if fixed_key:
                c4 = get_db_connection()
                try:
                    c4.execute("UPDATE files SET r2_key = ? WHERE short_id = ?", (fixed_key, short_id))
                    c4.commit()
                finally:
                    c4.close()
                fixed += 1
                continue

            backup_msg_id = entry.get("tg_backup_msg_id") or entry.get("message_id")
            if backup_msg_id and int(backup_msg_id or 0) > 0:
                c5 = get_db_connection()
                try:
                    c5.execute("UPDATE files SET r2_key = NULL, storage_type = 'telegram' WHERE short_id = ?", (short_id,))
                    c5.commit()
                finally:
                    c5.close()
                fixed += 1
                continue

            unfixed += 1

        except Exception:
            unfixed += 1

    return {"scanned": len(entries), "ok": ok, "broken": broken, "fixed": fixed, "unfixed": unfixed}

@app.post("/api/repair_r2")
async def repair_r2(key: str):
    verify_key(key)
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(_repair_r2_sync),
            timeout=300 
        )
        return {"status": "OK", **result}
    except asyncio.TimeoutError:
        return JSONResponse(status_code=504, content={"status": "timeout", "msg": "Repair took too long, run again"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})

@app.get("/api/repair_r2")
async def repair_r2_get(key: str):
    return await repair_r2(key)


# ============================================================
# 🛡️ SCANNER API ENDPOINTS
# ============================================================
@app.get("/api/scan/start")
async def start_scan_api(key: str, background_tasks: BackgroundTasks):
    verify_key(key)

    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
                if state.get("status") in ["running", "initializing"]:
                    return {"status": "Already scanning", "progress": state}
        except:
            pass

    def run_scanner_process():
        subprocess.Popen([sys.executable, "scan.py"])

    background_tasks.add_task(run_scanner_process)
    return {"status": "Scanning started"}

@app.get("/api/scan/progress")
async def scan_progress(key: str):
    verify_key(key) 
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except:
            return {"status": "reading_error"}
    return {"status": "idle"}

@app.post("/api/scan/delete")
async def scan_delete_file(key: str, data: dict = Body(...)):
    verify_key(key)
    file_code = data.get("file_code")
    if not file_code:
        raise HTTPException(status_code=400, detail="Missing file_code")

    result = await file_delete(key, file_code)
    return result

_client = None
_client_lock = asyncio.Lock()

async def get_client():
    global _client
    async with _client_lock:
        if _client and _client.is_connected(): return _client
        _client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
        await _client.start(bot_token=BOT_TOKEN)
        return _client

def verify_key(key: str):
    if key != INTERNAL_API_KEY: raise HTTPException(status_code=403)

@app.on_event("startup")
async def on_startup():
    init_db()
    asyncio.create_task(cache_cleanup_loop())
    asyncio.create_task(r2_deduplication_loop())
    async def auto_repair():
        await asyncio.sleep(30)
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(_repair_r2_sync),
                timeout=300
            )
            log(f"Auto-repair done: {result}")
        except Exception as e:
            log(f"Auto-repair error: {str(e)}")
    asyncio.create_task(auto_repair())
    log("URLKING HYBRID SYSTEM ONLINE & READY")
