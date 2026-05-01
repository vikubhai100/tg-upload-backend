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
from pathlib import Path
from urllib.parse import quote
from botocore.config import Config
from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Body
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
    import datetime
    line = f"{datetime.datetime.now().strftime('%H:%M:%S')} | {msg}"
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

# ============================================================
# 📁 FRONTEND SERVING
# ============================================================
CURRENT_FILE_PATH = Path(__file__).resolve()
FRONTEND_DIR = CURRENT_FILE_PATH.parent.parent / "frontend"

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_file = FRONTEND_DIR / "index.html"
    if index_file.exists():
        return index_file.read_text(encoding="utf-8")
    return "<h2>Frontend Not Found</h2>"

if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

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
        file_reference TEXT, dc_id INTEGER, storage_type TEXT DEFAULT 'telegram', r2_key TEXT
    )''')

    try: conn.execute("ALTER TABLE files ADD COLUMN storage_type TEXT DEFAULT 'telegram'")
    except: pass
    try: conn.execute("ALTER TABLE files ADD COLUMN r2_key TEXT")
    except: pass
    try: conn.execute("ALTER TABLE files ADD COLUMN last_accessed INTEGER DEFAULT 0")
    except: pass
    try: conn.execute("ALTER TABLE files ADD COLUMN r2_cache_key TEXT")
    except: pass

    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit(); conn.close()

def get_db_connection():
    c = sqlite3.connect(DB_FILE_SQLITE, check_same_thread=False)
    c.row_factory = sqlite3.Row; return c

def get_file_entry(short_id):
    with _db_lock:
        conn = get_db_connection()
        row = conn.execute("SELECT * FROM files WHERE short_id = ?", (short_id,)).fetchone()
        conn.close()
    return dict(row) if row else None

def save_file_entry(short_id, data):
    with _db_lock:
        conn = get_db_connection()
        existing = conn.execute("SELECT last_accessed, r2_cache_key FROM files WHERE short_id = ?", (short_id,)).fetchone()
        last_acc = existing["last_accessed"] if existing else int(time.time())
        cache_key = existing["r2_cache_key"] if existing else None

        conn.execute('''REPLACE INTO files (short_id, message_id, filename, size, content_type, channel_id, doc_id, access_hash, file_reference, dc_id, storage_type, r2_key, last_accessed, r2_cache_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (short_id, data.get("message_id", 0), data.get("filename"), data.get("size"),
             data.get("content_type"), data.get("channel_id", 0), str(data.get("doc_id", "0")),
             str(data.get("access_hash", "0")), str(data.get("file_reference", "0")),
             data.get("dc_id", 0), data.get("storage_type", "telegram"), data.get("r2_key"), last_acc, cache_key))
        conn.commit(); conn.close()

async def cache_cleanup_loop():
    await asyncio.sleep(60)
    while True:
        try:
            cutoff_time = int(time.time()) - (24 * 3600)
            conn = get_db_connection()
            rows = conn.execute("SELECT short_id, r2_cache_key FROM files WHERE r2_cache_key IS NOT NULL AND last_accessed < ?", (cutoff_time,)).fetchall()
            for row in rows:
                short_id, cache_key = row["short_id"], row["r2_cache_key"]
                try: r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=cache_key)
                except: pass
                conn.execute("UPDATE files SET r2_cache_key = NULL WHERE short_id = ?", (short_id,))
                conn.commit()
            conn.close()
        except: pass
        await asyncio.sleep(3600)

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
# 🧠 THE MASTER SPOOLER: TEMP FILE + CACHE (SAFE)
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
                with open(tmp_path, mode) as f_out:
                    async for chunk in client.iter_download(message.document, offset=current_offset, request_size=1024*1024):
                        f_out.write(chunk)
                        f_out.flush()

                        current_offset += len(chunk)
                        _active_dl[short_id]["dl_bytes"] = current_offset
                        await asyncio.sleep(0.01)

            except Exception as e:
                log(f"⚠️ Spooler TG Drop @ {format_size(current_offset)}. Retrying... Err: {e}")
                mode = "ab"
                await asyncio.sleep(2)

        _active_dl[short_id]["done"] = True
        log(f"✅ SPOOLER DONE | {short_id} fully saved to Temp! Uploading to R2...")

        r2_cache_key = f"cache_{short_id}_{uuid.uuid4().hex[:6]}"
        def s3_up():
            r2_client.upload_file(tmp_path, R2_BUCKET_NAME, r2_cache_key, ExtraArgs={'ContentType': entry["content_type"] or "application/octet-stream"})
        await asyncio.to_thread(s3_up)

        conn = get_db_connection()
        conn.execute("UPDATE files SET r2_cache_key = ? WHERE short_id = ?", (r2_cache_key, short_id))
        conn.commit(); conn.close()
        log(f"☁️ R2 CACHE SUCCESS | {short_id} cached perfectly!")

    except Exception as e:
        if short_id in _active_dl: _active_dl[short_id]["err"] = True
        log(f"❌ FATAL Spooler Error for {short_id}: {e}")
    finally:
        await asyncio.sleep(1800)
        if short_id in _active_dl: _active_dl.pop(short_id, None)
        try: os.remove(tmp_path)
        except: pass

@app.get("/download/{short_id}")
async def download_handle(request: Request, short_id: str):
    client_ip = get_client_ip(request)
    entry = get_file_entry(short_id)
    if not entry: raise HTTPException(status_code=404, detail="File Not Found")

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
    log(f"⬇️ SPOOL STREAM START | {filename_raw} | IP: {client_ip}")

    async def temp_file_streamer():
        with open(tmp_path, "rb") as f:
            f.seek(start_byte)
            curr = start_byte

            while curr <= end_byte:
                if await request.is_disconnected(): break

                info = _active_dl.get(short_id)
                if not info:
                    break

                target_bytes = info["dl_bytes"]

                while curr >= target_bytes:
                    info = _active_dl.get(short_id)
                    if not info or info["done"]: break
                    if info["err"]:
                        raise RuntimeError("Backend connection to Telegram dropped")

                    await asyncio.sleep(0.2)
                    target_bytes = _active_dl.get(short_id, {}).get("dl_bytes", 0)

                info = _active_dl.get(short_id, {})
                if info.get("done", False) and curr >= target_bytes:
                    break

                avail = target_bytes - curr
                read_size = min(128 * 1024, end_byte - curr + 1)
                if not info.get("done", False):
                    read_size = min(read_size, avail)

                if read_size > 0:
                    data = f.read(read_size)
                    if not data:
                        await asyncio.sleep(0.1)
                        f.seek(curr)
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
    if size < 10*1024*1024: return await client.upload_file(file_path)
    f_id = int.from_bytes(os.urandom(8), "big", signed=True)
    parts = math.ceil(size / (512*1024))
    sem = asyncio.Semaphore(15)
    async def up_part(idx):
        async with sem:
            with open(file_path, 'rb') as f:
                f.seek(idx * 512*1024)
                chunk = f.read(512*1024)
            await client(SaveBigFilePartRequest(f_id, idx, parts, chunk))
    await asyncio.gather(*[up_part(i) for i in range(parts)])
    return InputFileBig(id=f_id, parts=parts, name=name)

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
                with open(tmp_path, 'wb') as f:
                    async for chunk in r.content.iter_chunked(5*1024*1024): f.write(chunk)
        client = await get_client()
        up_file = await parallel_upload(client, tmp_path)
        msg = await client.send_file(CHANNEL_ID, up_file, force_document=True)
        short_id = str(uuid.uuid4())[:8]
        save_file_entry(short_id, {
            "message_id": msg.id, "filename": filename, "size": os.path.getsize(tmp_path),
            "content_type": "application/octet-stream", "channel_id": CHANNEL_ID,
            "doc_id": msg.document.id, "access_hash": msg.document.access_hash,
            "file_reference": msg.document.file_reference.hex(), "dc_id": msg.document.dc_id
        })
        os.unlink(tmp_path)
        return [{"file_code": short_id, "file_status": "OK"}]
    except Exception as e: return {"error": str(e)}

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
        if entry.get("storage_type") == "r2" and entry.get("r2_key"):
            try: r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=entry["r2_key"])
            except: pass
        if entry.get("r2_cache_key"):
            try: r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=entry["r2_cache_key"])
            except: pass

    with _db_lock:
        conn = sqlite3.connect(DB_FILE_SQLITE); conn.execute("DELETE FROM files WHERE short_id = ?", (file_code,))
        conn.commit(); conn.close()
    return {"status": 200, "msg": "OK"}

@app.post("/api/file/register_r2")
async def register_r2(key: str, data: dict = Body(...)):
    verify_key(key)
    save_file_entry(data["short_id"], {
        "filename": data["filename"], "size": data["size"], "storage_type": "r2",
        "r2_key": data["r2_key"], "content_type": "application/octet-stream"
    })
    return {"status": "OK"}

# ============================================================
# ✏️ FILE RENAME — NODE BOT YAHI CALL KARTA HAI
# ============================================================
@app.get("/api/file/rename")
async def file_rename(key: str, file_code: str, name: str):
    """
    Node bot se call hota hai jab user file rename karta hai.
    DB mein filename update karta hai taaki download pe naya naam aaye.
    """
    verify_key(key)

    # Sanitize: dangerous characters hata do
    safe_name = name.strip().replace("/", "_").replace("\\", "_").replace("\x00", "")
    if not safe_name:
        raise HTTPException(status_code=400, detail="Invalid filename")

    with _db_lock:
        conn = get_db_connection()
        result = conn.execute(
            "UPDATE files SET filename = ? WHERE short_id = ?",
            (safe_name, file_code)
        )
        conn.commit()
        changed = result.rowcount
        conn.close()

    if changed == 0:
        log(f"⚠️ RENAME MISS | {file_code} not found in DB")
        return {"status": 404, "msg": "File not found"}

    log(f"✏️ RENAME OK | {file_code} → {safe_name}")
    return {"status": 200, "msg": "OK", "new_name": safe_name}

# ============================================================
# ℹ️ FILE INFO
# ============================================================
@app.get("/api/file/info")
async def file_info(key: str, file_code: str):
    verify_key(key)
    entry = get_file_entry(file_code)
    if entry:
        return {"result": [{"name": entry["filename"], "size": entry["size"], "storage": entry.get("storage_type", "telegram")}]}
    return {"result": []}

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

@app.get("/files")
async def list_files(key: str, page: int = 1, limit: int = 10):
    verify_key(key); conn = get_db_connection()
    offset = (page - 1) * limit
    total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    rows = conn.execute("SELECT * FROM files ORDER BY rowid DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    conn.close()
    return {
        "files": [{"short_id": r["short_id"], "filename": r["filename"], "size": format_size(r["size"]), "download_link": f"{BASE_URL}/download/{r['short_id']}"} for r in rows],
        "total": total, "page": page, "total_pages": math.ceil(total / limit) if total > 0 else 1
    }

@app.on_event("startup")
async def on_startup():
    init_db()
    asyncio.create_task(cache_cleanup_loop())
    log("✅ URLKING HYBRID SYSTEM (SPOOLER V3) ONLINE & READY")
