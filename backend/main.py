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
import aiohttp
import hashlib
import hmac      
import aiofiles
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

def get_client_ip(request: Request):
    fwd = request.headers.get("X-Forwarded-For")
    return fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "Unknown")

app = FastAPI(title="URLKING Hybrid Storage")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
BOT_TOKEN        = os.getenv("BOT_TOKEN", "")
API_ID           = int(os.getenv("API_ID", "0"))
API_HASH         = os.getenv("API_HASH", "")
CHANNEL_ID       = int(os.getenv("CHANNEL_ID", "0"))
BASE_URL         = os.getenv("BASE_URL", "https://db.mypdftools.site")
SESSION_STR      = os.getenv("SESSION_STRING", "")
DB_FILE_SQLITE   = "/app/data/files.db"
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "super_secret_key_123")
DOWNLOAD_SECRET  = "URLKING_ANTI_BOT_SECRET_2024"  # 🔥 YE NAYI LINE ADD KARO
# ============================================================
# 🛡️ DEDUPLICATION HELPER
# ============================================================
def calculate_hash(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192 * 1024): 
            hasher.update(chunk)
    return hasher.hexdigest()

# ============================================================
# 📁 FRONTEND SERVING
# ============================================================
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
# 🗄️ DATABASE MANAGEMENT
# ============================================================
_db_lock = threading.Lock()

def init_db():
    os.makedirs(os.path.dirname(DB_FILE_SQLITE), exist_ok=True)
    conn = sqlite3.connect(DB_FILE_SQLITE)
    conn.execute('''CREATE TABLE IF NOT EXISTS files (
        short_id TEXT PRIMARY KEY, message_id INTEGER, filename TEXT, size INTEGER,
        content_type TEXT, channel_id INTEGER, doc_id TEXT, access_hash TEXT,
        file_reference TEXT, dc_id INTEGER, storage_type TEXT DEFAULT 'telegram', r2_key TEXT,
        last_accessed INTEGER DEFAULT 0, r2_cache_key TEXT, file_hash TEXT
    )''')
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit(); conn.close()

def get_db_connection():
    c = sqlite3.connect(DB_FILE_SQLITE, check_same_thread=False)
    c.row_factory = sqlite3.Row; return c

def save_file_entry(short_id, data):
    with _db_lock:
        conn = get_db_connection()
        existing = conn.execute("SELECT last_accessed, r2_cache_key FROM files WHERE short_id = ?", (short_id,)).fetchone()
        last_acc = existing["last_accessed"] if existing else int(time.time())
        cache_key = data.get("r2_cache_key") or (existing["r2_cache_key"] if existing else None)
        file_hash = data.get("file_hash")

        conn.execute('''REPLACE INTO files (short_id, message_id, filename, size, content_type, channel_id, doc_id, access_hash, file_reference, dc_id, storage_type, r2_key, last_accessed, r2_cache_key, file_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (short_id, data.get("message_id", 0), data.get("filename"), data.get("size"),
             data.get("content_type"), data.get("channel_id", 0), str(data.get("doc_id", "0")),
             str(data.get("access_hash", "0")), str(data.get("file_reference", "0")),
             data.get("dc_id", 0), data.get("storage_type", "telegram"), data.get("r2_key"), last_acc, cache_key, file_hash))
        conn.commit(); conn.close()

def get_file_entry(short_id):
    with _db_lock:
        conn = get_db_connection()
        row = conn.execute("SELECT * FROM files WHERE short_id = ?", (short_id,)).fetchone()
        conn.close()
    return dict(row) if row else None

# ============================================================
# 🧹 AUTO-CLEANUP 1: R2 DEDUPLICATION
# ============================================================
def execute_r2_cleanup():
    try:
        log("🧹 [AUTO-CLEANUP] Starting Deep R2 Deduplication scan...")
        conn = get_db_connection()
        total_saved_bytes = 0
        files_cleaned = 0

        # PASS 1: Deduplicate by exact FILE HASH
        duplicates_hash = conn.execute('''
            SELECT file_hash, COUNT(*) as c FROM files 
            WHERE storage_type = 'r2' AND file_hash IS NOT NULL AND file_hash != '' AND r2_key IS NOT NULL
            GROUP BY file_hash HAVING c > 1
        ''').fetchall()

        for dup in duplicates_hash:
            f_hash = dup["file_hash"]
            rows = conn.execute("SELECT short_id, r2_key, size FROM files WHERE file_hash = ? AND storage_type = 'r2' ORDER BY rowid ASC", (f_hash,)).fetchall()
            if len(rows) > 1:
                master_r2_key = rows[0]["r2_key"]
                for i in range(1, len(rows)):
                    dup_short_id = rows[i]["short_id"]
                    dup_r2_key = rows[i]["r2_key"]
                    if dup_r2_key != master_r2_key:
                        conn.execute("UPDATE files SET r2_key = ? WHERE short_id = ?", (master_r2_key, dup_short_id))
                        try:
                            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=dup_r2_key)
                            total_saved_bytes += rows[i]["size"]
                            files_cleaned += 1
                            log(f"🗑️ [DEDUPE] Deleted extra R2 file: {dup_r2_key}")
                        except: pass

        # PASS 2: Deduplicate by exact NAME + exact SIZE
        duplicates_name_size = conn.execute('''
            SELECT filename, size, COUNT(*) as c FROM files 
            WHERE storage_type = 'r2' AND r2_key IS NOT NULL AND (file_hash IS NULL OR file_hash = '')
            GROUP BY filename, size HAVING c > 1
        ''').fetchall()

        for dup in duplicates_name_size:
            f_name = dup["filename"]
            f_size = dup["size"]
            rows = conn.execute("SELECT short_id, r2_key, size FROM files WHERE filename = ? AND size = ? AND storage_type = 'r2' ORDER BY rowid ASC", (f_name, f_size)).fetchall()
            if len(rows) > 1:
                master_r2_key = rows[0]["r2_key"]
                for i in range(1, len(rows)):
                    dup_short_id = rows[i]["short_id"]
                    dup_r2_key = rows[i]["r2_key"]
                    if dup_r2_key != master_r2_key:
                        conn.execute("UPDATE files SET r2_key = ? WHERE short_id = ?", (master_r2_key, dup_short_id))
                        try:
                            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=dup_r2_key)
                            total_saved_bytes += rows[i]["size"]
                            files_cleaned += 1
                            log(f"🗑️ [DEDUPE] Deleted extra R2 file: {dup_r2_key}")
                        except: pass

        conn.commit()
        conn.close()
        
        if files_cleaned > 0:
            log(f"✅ [AUTO-CLEANUP] Done! Removed {files_cleaned} duplicates. Freed {format_size(total_saved_bytes)} space!")
            
    except Exception as e:
        log(f"❌ [AUTO-CLEANUP ERROR]: {str(e)}")

# ============================================================
# 🧹 AUTO-CLEANUP 2: PHYSICAL CACHE SWEEPER (NEW BUG FIX)
# ============================================================
def execute_cache_sweeper():
    try:
        log("🧹 [CACHE SWEEPER] Scanning R2 for expired 24h+ cache files...")
        # 24 ghante purani date nikalna
        cutoff_date = datetime.now(timezone.utc) - timedelta(hours=24)
        
        # S3 Pagination use kar rahe hain taaki R2 crash na ho
        paginator = r2_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix='cache_')
        
        cleaned_cache = 0
        conn = get_db_connection()

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Agar file ka physical creation date 24h se purana hai
                    if obj['LastModified'] < cutoff_date:
                        cache_key = obj['Key']
                        try:
                            # 1. R2 Se uda do
                            r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=cache_key)
                            # 2. Database ko safed jhoot bolne se roko (nullify karo)
                            conn.execute("UPDATE files SET r2_cache_key = NULL WHERE r2_cache_key = ?", (cache_key,))
                            cleaned_cache += 1
                            log(f"🗑️ [CACHE SWEEPER] Destroyed old cache: {cache_key}")
                        except: pass
        
        conn.commit()
        conn.close()

        if cleaned_cache > 0:
            log(f"✅ [CACHE SWEEPER] Successfully wiped {cleaned_cache} expired cache files from R2!")
        else:
            log("✅ [CACHE SWEEPER] R2 Cache is already spotless.")
            
    except Exception as e:
        log(f"❌ [CACHE SWEEPER ERROR]: {str(e)}")

# --- Background Loops ---
async def r2_deduplication_loop():
    await asyncio.sleep(60) 
    while True:
        await asyncio.to_thread(execute_r2_cleanup)
        await asyncio.sleep(86400) # Every 24 hours

async def cache_cleanup_loop():
    await asyncio.sleep(120) 
    while True:
        await asyncio.to_thread(execute_cache_sweeper)
        await asyncio.sleep(12 * 3600) # Har 12 ghante me chalega 24h purani files dhundne

@app.get("/api/run_cleanup")
async def trigger_manual_cleanup(key: str, background_tasks: BackgroundTasks):
    verify_key(key)
    # Ab is button ko dabane se Deduplication AND Cache dono clean honge ek sath!
    background_tasks.add_task(execute_r2_cleanup)
    background_tasks.add_task(execute_cache_sweeper)
    return {"status": "success", "message": "Deep R2 Cleanup & Cache Sweeper started in the background!"}

def redirect_to_r2(r2_key, filename, client_ip, log_tag="REDIRECT"):
    try:
        url = r2_client.generate_presigned_url('get_object', Params={
            'Bucket': R2_BUCKET_NAME, 'Key': r2_key,
            'ResponseContentDisposition': f"attachment; filename=\"{filename}\""
        }, ExpiresIn=7200)
        log(f"🚀 R2 {log_tag} | {filename} | IP: {client_ip}")
        return RedirectResponse(url=url)
    except Exception as e: raise HTTPException(status_code=500)

# ============================================================
# 🧠 THE MASTER SPOOLER: TEMP FILE + CACHE
# ============================================================
_active_dl = {}

async def bg_fetch_and_cache(short_id, entry):
    tmp_path = f"/tmp/dl_{short_id}.bin"
    file_size = int(entry["size"])
    current_offset = 0
    mode = "wb"

    try:
        log(f"⚙️ SPOOLER START | Fetching {short_id} ({format_size(file_size)}) to Temp...")
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
            except Exception as e:
                log(f"⚠️ Spooler TG Drop @ {format_size(current_offset)}. Retrying... Err: {e}")
                mode = "ab"
                await asyncio.sleep(2)

        _active_dl[short_id]["done"] = True
        r2_cache_key = f"cache_{short_id}_{uuid.uuid4().hex[:6]}"
        def s3_up():
            r2_client.upload_file(tmp_path, R2_BUCKET_NAME, r2_cache_key, ExtraArgs={'ContentType': entry["content_type"] or "application/octet-stream"})
        await asyncio.to_thread(s3_up)

        conn = get_db_connection()
        conn.execute("UPDATE files SET r2_cache_key = ? WHERE short_id = ?", (r2_cache_key, short_id))
        doc_id = entry.get("doc_id")
        if doc_id:
            conn.execute("UPDATE files SET r2_cache_key = ? WHERE doc_id = ? AND r2_cache_key IS NULL", (r2_cache_key, doc_id))
        conn.commit(); conn.close()

    except Exception as e:
        if short_id in _active_dl: _active_dl[short_id]["err"] = True
    finally:
        await asyncio.sleep(1800)
        if short_id in _active_dl: _active_dl.pop(short_id, None)
        try: os.remove(tmp_path)
        except: pass

@app.get("/download/{short_id}")
async def download_handle(request: Request, short_id: str, exp: int = 0, sign: str = ""):
    
    # User ki IP pehle hi nikal lo
    client_ip = get_client_ip(request)

    # 🔴 ANTI-BOT SECURITY WALL START 🔴
    if not exp or not sign:
        return HTMLResponse(
            content="<div style='font-family:sans-serif; text-align:center; margin-top:50px; color:#d9534f;'>"
                    "<h2>❌ Access Denied</h2>"
                    "<p>Direct linking or bots are blocked. Please generate the link from the official website.</p></div>", 
            status_code=403
        )

    if int(time.time()) > exp:
        return HTMLResponse(
            content="<div style='font-family:sans-serif; text-align:center; margin-top:50px; color:#f0ad4e;'>"
                    "<h2>⏳ Link Expired</h2>"
                    "<p>Your download link has expired. Please go back and generate a new link.</p></div>", 
            status_code=403
        )

    # Validate signature WITH IP ADDRESS
    data_to_sign = f"{short_id}:{exp}:{client_ip}".encode('utf-8')
    expected_sign = hmac.new(DOWNLOAD_SECRET.encode('utf-8'), data_to_sign, hashlib.sha256).hexdigest()

    if not hmac.compare_digest(expected_sign, sign):
        return HTMLResponse(
            content="<div style='font-family:sans-serif; text-align:center; margin-top:50px; color:#d9534f;'>"
                    "<h2>🛑 IP Mismatch or Invalid Link!</h2>"
                    "<p>Link sharing is strictly prohibited. Please open the website and verify yourself.</p></div>", 
            status_code=403
        )
    # 🔴 ANTI-BOT SECURITY WALL END 🔴

    entry = get_file_entry(short_id)
    if not entry: raise HTTPException(status_code=404, detail="File Not Found")

    # === Iske aage ka purana DB update aur file streaming code waisa hi rahega jaisa tha ===
    conn = get_db_connection()
    conn.execute("UPDATE files SET last_accessed = ? WHERE short_id = ?", (int(time.time()), short_id))
    conn.commit(); conn.close()

    if entry.get("storage_type") == "r2" and entry.get("r2_key"):
        return redirect_to_r2(entry["r2_key"], entry["filename"], client_ip, "PERMANENT")
    if entry.get("r2_cache_key"):
        return redirect_to_r2(entry["r2_cache_key"], entry["filename"], client_ip, "CACHED LINK")

    tmp_path = f"/tmp/dl_{short_id}.bin"
    if short_id not in _active_dl:
        _active_dl[short_id] = {"dl_bytes": 0, "done": False, "err": False}
        asyncio.create_task(bg_fetch_and_cache(short_id, entry))

    for _ in range(50):
        if os.path.exists(tmp_path) and _active_dl.get(short_id, {}).get("dl_bytes", 0) > 0:
            break
        await asyncio.sleep(0.2)

    if not os.path.exists(tmp_path):
        raise HTTPException(500, "Failed to connect to Telegram Core")

    file_size = int(entry["size"])
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
                    if info["err"]: raise RuntimeError("Backend connection dropped")
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
        "Content-Type": content_type, "Content-Length": str(content_length),
        "Accept-Ranges": "bytes", "X-Accel-Buffering": "no"
    }
    if range_header: headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{file_size}"
    return StreamingResponse(temp_file_streamer(), status_code=206 if range_header else 200, headers=headers)



# ============================================================
# 🚀 UPLOAD & REMOTE LOGIC
# ============================================================
async def parallel_upload(client, file_path):
    size = os.path.getsize(file_path)
    name = os.path.basename(file_path)
    if size < 10*1024*1024: 
        return await client.upload_file(file_path)

    f_id = int.from_bytes(os.urandom(8), "big", signed=True)
    parts = math.ceil(size / (512*1024))
    sem = asyncio.Semaphore(4) 

    async def up_part(idx):
        async with sem:
            async with aiofiles.open(file_path, 'rb') as f:
                await f.seek(idx * 512*1024)
                chunk = await f.read(512*1024)
            await client(SaveBigFilePartRequest(f_id, idx, parts, chunk))

    await asyncio.gather(*[up_part(i) for i in range(parts)])
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
                file_obj = v
                break

        if not file_obj:
            return JSONResponse(status_code=400, content=[{"error": "No file detected"}])

        filename = file_obj.filename
        content_type = getattr(file_obj, "content_type", "application/octet-stream")
        tmp_path = f"/tmp/web_{uuid.uuid4().hex[:8]}.bin"

        async with aiofiles.open(tmp_path, "wb") as f:
            while chunk := await file_obj.read(2 * 1024 * 1024):
                await f.write(chunk)

        file_hash = await asyncio.to_thread(calculate_hash, tmp_path)
        conn = get_db_connection()
        existing = conn.execute("SELECT * FROM files WHERE file_hash = ?", (file_hash,)).fetchone()
        conn.close()

        if existing:
            os.unlink(tmp_path)
            new_id = str(uuid.uuid4())[:8]
            save_file_entry(new_id, {
                "message_id": existing["message_id"], "filename": filename, "size": existing["size"],
                "content_type": content_type, "channel_id": existing["channel_id"],
                "doc_id": existing["doc_id"], "access_hash": existing["access_hash"],
                "file_reference": existing["file_reference"], "dc_id": existing["dc_id"],
                "storage_type": existing["storage_type"], "r2_key": existing["r2_key"],
                "file_hash": file_hash, "r2_cache_key": existing.get("r2_cache_key")
            })
            log(f"♻️ WEB DUPLICATE CLONED | Reused ID: {new_id}")
            return JSONResponse(content=[{"file_code": new_id, "file_status": "OK"}])

        client = await get_client()
        file_size = os.path.getsize(tmp_path)

        if file_size < 10 * 1024 * 1024:
            msg = await client.send_file(CHANNEL_ID, tmp_path, force_document=True)
        else:
            up_file = await parallel_upload(client, tmp_path)
            msg = await client.send_file(CHANNEL_ID, up_file, force_document=True)

        short_id = str(uuid.uuid4())[:8]
        save_file_entry(short_id, {
            "message_id": msg.id, "filename": filename, "size": file_size,
            "content_type": content_type, "channel_id": CHANNEL_ID,
            "doc_id": msg.document.id, "access_hash": msg.document.access_hash,
            "file_reference": msg.document.file_reference.hex(), "dc_id": msg.document.dc_id,
            "file_hash": file_hash, "storage_type": "telegram"
        })
        os.unlink(tmp_path)
        log(f"✅ NEW WEB UPLOAD | ID: {short_id}")
        return JSONResponse(content=[{"file_code": short_id, "file_status": "OK"}])

    except Exception as e:
        log(f"❌ API UPLOAD CRASH: {str(e)}")
        return JSONResponse(status_code=500, content=[{"error": str(e)}])

@app.post("/api/remote_upload")
async def remote_upload(request: Request):
    try:
        data = await request.json()
        verify_key(data.get("key"))
        url = data.get("url")
        filename = data.get("filename", f"file_{int(time.time())}.bin")
        tmp_path = f"/tmp/{uuid.uuid4()}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as r:
                async with aiofiles.open(tmp_path, 'wb') as f:
                    async for chunk in r.content.iter_chunked(5*1024*1024): 
                        await f.write(chunk)

        file_hash = await asyncio.to_thread(calculate_hash, tmp_path)
        conn = get_db_connection()
        existing = conn.execute("SELECT * FROM files WHERE file_hash = ?", (file_hash,)).fetchone()
        conn.close()

        if existing:
            os.unlink(tmp_path)
            new_short_id = str(uuid.uuid4())[:8]
            save_file_entry(new_short_id, {
                "message_id": existing["message_id"], "filename": filename, "size": existing["size"],
                "content_type": existing["content_type"], "channel_id": existing["channel_id"],
                "doc_id": existing["doc_id"], "access_hash": existing["access_hash"],
                "file_reference": existing["file_reference"], "dc_id": existing["dc_id"],
                "storage_type": existing["storage_type"], "r2_key": existing["r2_key"],
                "file_hash": file_hash, "r2_cache_key": existing.get("r2_cache_key")
            })
            log(f"♻️ REMOTE DUPLICATE CLONED | Reused ID: {new_short_id}")
            return [{"file_code": new_short_id, "file_status": "OK"}]

        client = await get_client()
        up_file = await parallel_upload(client, tmp_path)
        msg = await client.send_file(CHANNEL_ID, up_file, force_document=True)
        short_id = str(uuid.uuid4())[:8]

        save_file_entry(short_id, {
            "message_id": msg.id, "filename": filename, "size": os.path.getsize(tmp_path),
            "content_type": "application/octet-stream", "channel_id": CHANNEL_ID,
            "doc_id": msg.document.id, "access_hash": msg.document.access_hash,
            "file_reference": msg.document.file_reference.hex(), "dc_id": msg.document.dc_id,
            "file_hash": file_hash
        })
        os.unlink(tmp_path)
        log(f"✅ NEW REMOTE UPLOAD | ID: {short_id}")
        return [{"file_code": short_id, "file_status": "OK"}]
    except Exception as e: return {"error": str(e)}

# ============================================================
# ⚡ INSTANT R2 REGISTRATION DEDUPE
# ============================================================
@app.post("/api/file/register_r2")
async def register_r2(key: str, background_tasks: BackgroundTasks, data: dict = Body(...)):
    verify_key(key)
    
    file_hash = data.get("file_hash")
    new_r2_key = data["r2_key"]
    filename = data.get("filename")
    size = data.get("size", 0)
    
    conn = get_db_connection()
    existing = None
    
    if file_hash and file_hash.strip():
        existing = conn.execute("SELECT r2_key FROM files WHERE file_hash = ? AND storage_type = 'r2' AND r2_key IS NOT NULL", (file_hash,)).fetchone()
    
    if not existing and filename and size > 0:
        existing = conn.execute("SELECT r2_key FROM files WHERE filename = ? AND size = ? AND storage_type = 'r2' AND r2_key IS NOT NULL", (filename, size)).fetchone()
        
    conn.close()
    
    if existing:
        master_r2_key = existing["r2_key"]
        if master_r2_key != new_r2_key:
            save_file_entry(data["short_id"], {
                "filename": filename, "size": size, "storage_type": "r2",
                "r2_key": master_r2_key, "content_type": "application/octet-stream",
                "file_hash": file_hash
            })
            
            def delete_redundant():
                try:
                    r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=new_r2_key)
                    log(f"🗑️ [INSTANT DEDUPE] Deleted redundant R2 upload: {new_r2_key}")
                except: pass
                
            background_tasks.add_task(delete_redundant)
            return {"status": "OK", "msg": "Duplicate handled instantly"}
            
    save_file_entry(data["short_id"], {
        "filename": filename, "size": size, "storage_type": "r2",
        "r2_key": new_r2_key, "content_type": "application/octet-stream",
        "file_hash": file_hash
    })
    return {"status": "OK"}

# ============================================================
# 📑 FILE MANAGEMENT
# ============================================================
@app.get("/api/file/clone")
async def file_clone(key: str, file_code: str):
    verify_key(key); entry = get_file_entry(file_code)
    if not entry: return {"status": 404}
    new_id = str(uuid.uuid4())[:8]
    save_file_entry(new_id, entry)
    return {"status": 200, "result": {"filecode": new_id}}

@app.get("/api/file/delete")
async def file_delete(key: str, file_code: str):
    verify_key(key); entry = get_file_entry(file_code)
    if entry:
        with _db_lock:
            conn = get_db_connection()
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
            conn.commit(); conn.close()
    return {"status": 200, "msg": "OK"}

@app.get("/api/file/rename")
async def file_rename(key: str, file_code: str, name: str):
    verify_key(key)
    safe_name = name.strip().replace("/", "_").replace("\\", "_").replace("\x00", "")
    if not safe_name: raise HTTPException(status_code=400, detail="Invalid filename")

    with _db_lock:
        conn = get_db_connection()
        result = conn.execute("UPDATE files SET filename = ? WHERE short_id = ?", (safe_name, file_code))
        conn.commit(); changed = result.rowcount; conn.close()

    if changed == 0: return {"status": 404, "msg": "File not found"}
    return {"status": 200, "msg": "OK", "new_name": safe_name}

@app.get("/api/file/info")
async def file_info(key: str, file_code: str):
    verify_key(key); entry = get_file_entry(file_code)
    if entry: return {"result": [{"name": entry["filename"], "size": entry["size"], "storage": entry.get("storage_type", "telegram")}]}
    return {"result": []}

@app.get("/api/index_forwarded")
async def index_forwarded(key: str, message_id: int, filename: str):
    verify_key(key)
    client = await get_client()
    message = await client.get_messages(CHANNEL_ID, ids=message_id)
    if not message or not message.document: return {"error": "Not Found"}

    doc_id_str = str(message.document.id)
    conn = get_db_connection()
    existing = conn.execute("SELECT * FROM files WHERE doc_id = ?", (doc_id_str,)).fetchone()
    conn.close()

    if existing:
        try: await client.delete_messages(CHANNEL_ID, [message_id])
        except: pass
        new_id = str(uuid.uuid4())[:8]
        save_file_entry(new_id, {
            "message_id": existing["message_id"], "filename": filename, "size": existing["size"],
            "content_type": existing["content_type"], "channel_id": existing["channel_id"],
            "doc_id": existing["doc_id"], "access_hash": existing["access_hash"],
            "file_reference": existing["file_reference"], "dc_id": existing["dc_id"],
            "storage_type": existing["storage_type"], "r2_key": existing["r2_key"],
            "file_hash": existing.get("file_hash"), "r2_cache_key": existing.get("r2_cache_key")
        })
        return [{"file_code": new_id, "file_status": "OK"}]

    short_id = str(uuid.uuid4())[:8]
    save_file_entry(short_id, {
        "message_id": message.id, "filename": filename, "size": message.document.size,
        "content_type": message.file.mime_type, "channel_id": CHANNEL_ID,
        "doc_id": message.document.id, "access_hash": message.document.access_hash,
        "file_reference": message.document.file_reference.hex(), "dc_id": message.document.dc_id,
        "storage_type": "telegram"
    })
    return [{"file_code": short_id, "file_status": "OK"}]

@app.get("/files")
async def list_files(key: str, page: int = 1, limit: int = 10):
    verify_key(key)
    conn = get_db_connection()
    offset = (page - 1) * limit
    total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    rows = conn.execute("SELECT * FROM files ORDER BY rowid DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    conn.close()
    return {
        "files": [{"short_id": r["short_id"], "filename": r["filename"], "size": format_size(r["size"]), "download_link": f"{BASE_URL}/download/{r['short_id']}"} for r in rows],
        "total": total, "page": page, "total_pages": math.ceil(total / limit) if total > 0 else 1
    }

# ============================================================
# 🛠️ UTILITIES & STARTUP
# ============================================================
_client = None
async def get_client():
    global _client
    if _client and _client.is_connected(): return _client
    _client = TelegramClient(StringSession(SESSION_STR), API_ID, API_HASH)
    await _client.start(bot_token=BOT_TOKEN); return _client

def verify_key(key: str):
    if key != INTERNAL_API_KEY: raise HTTPException(status_code=403)

@app.on_event("startup")
async def on_startup():
    init_db()
    asyncio.create_task(cache_cleanup_loop())
    asyncio.create_task(r2_deduplication_loop()) 
    log("✅ URLKING HYBRID SYSTEM (SPOOLER V3) ONLINE & READY")
