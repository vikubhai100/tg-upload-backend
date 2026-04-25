import os
import uuid
import tempfile
import asyncio
import time
import sqlite3
import threading
import math
from pathlib import Path
from urllib.parse import quote
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
import sys
import aiohttp
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import InputFileBig
from telethon.tl.functions.upload import SaveBigFilePartRequest

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
# TELEGRAM CLIENT (Optimized for Multi-Thread)
# ============================================================
_client = None

async def get_client():
    global _client
    if _client and _client.is_connected():
        return _client
    session = StringSession(SESSION_STR) if SESSION_STR else StringSession()
    
    # ⚡ Speed & Connection Fixes
    _client = TelegramClient(
        session, API_ID, API_HASH,
        connection_retries=15,
        retry_delay=2,
        request_retries=10, 
        flood_sleep_threshold=15, # Chote flood waits ko ignore karega
    )
    await _client.start(bot_token=BOT_TOKEN)
    return _client

def format_size(size_bytes):
    if size_bytes == 0: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

# ============================================================
# DATABASE SETUP
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
        dc_id       INTEGER
    )''')
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
        conn.execute('''REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (short_id, data.get("message_id"), data.get("filename"), data.get("size"),
             data.get("content_type"), data.get("channel_id"), str(data.get("doc_id")),
             str(data.get("access_hash")), str(data.get("file_reference")), data.get("dc_id")))
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
        await get_client()
        log("✅ Telegram connected!")
    except Exception as e:
        log(f"⚠️ Telegram connect failed at startup: {e}")

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content="<h1>TeleStore Running</h1>")

# ============================================================
# ⚡ UPLOAD LOGIC
# ============================================================
async def parallel_upload(client, file_path):
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    if file_size < 10 * 1024 * 1024:
        return await client.upload_file(file_path, part_size_kb=512)

    part_size   = 512 * 1024
    total_parts = math.ceil(file_size / part_size)
    file_id     = int.from_bytes(os.urandom(8), "big", signed=True)
    sem         = asyncio.Semaphore(15)

    async def upload_part(part_idx):
        async with sem:
            start = part_idx * part_size
            end   = min(start + part_size, file_size)
            with open(file_path, 'rb') as f:
                f.seek(start)
                chunk = f.read(end - start)
            await client(SaveBigFilePartRequest(file_id, part_idx, total_parts, chunk))

    tasks = [asyncio.create_task(upload_part(i)) for i in range(total_parts)]
    await asyncio.gather(*tasks)
    return InputFileBig(id=file_id, parts=total_parts, name=file_name)

# ============================================================
# ⚡ DOWNLOAD LOGIC (CHROME & ADM FULLY SUPPORTED)
# ============================================================
@app.get("/download/{short_id}")
async def download_file(request: Request, short_id: str):
    entry = get_file_entry(short_id)
    if not entry: raise HTTPException(status_code=404, detail="File not found")

    file_size    = int(entry["size"])
    filename_raw = entry["filename"]
    content_type = entry["content_type"] or "application/octet-stream"

    # --- 🛠️ Chrome & Browser Math Fix ---
    range_header = request.headers.get("Range")
    start_byte = 0
    end_byte = file_size - 1

    if range_header:
        try:
            range_str = range_header.replace("bytes=", "").split("-")
            start_byte = int(range_str[0]) if range_str[0] else 0
            if len(range_str) > 1 and range_str[1]:
                end_byte = int(range_str[1])
        except Exception:
            start_byte = 0
            end_byte = file_size - 1

    # Agar browser file size ke bahar ka data maange
    if start_byte >= file_size:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    content_length = end_byte - start_byte + 1

    try:
        client  = await get_client()
        message = await client.get_messages(entry["channel_id"], ids=entry["message_id"])
        if not message or not message.document:
            raise HTTPException(status_code=404, detail="File deleted from Telegram")
        
        document = message.document

        async def stream_direct():
            try:
                async for chunk in client.iter_download(
                    document,
                    offset=start_byte,
                    request_size=4 * 1024 * 1024, # 4MB keeps browsers stable without timeout
                ):
                    yield bytes(chunk)
            except asyncio.CancelledError:
                # Browser ne download pause ya cancel kar diya (No Crash)
                pass
            except Exception as e:
                log(f"Stream error: {e}")

        encoded_filename = quote(filename_raw)
        
        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}",
            "Content-Type": content_type,
            "Content-Length": str(content_length),
            "Accept-Ranges": "bytes",
            "X-Accel-Buffering": "no", 
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        }
        
        # Proper HTTP 206 execution for Chrome
        if range_header:
            headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{file_size}"
            return StreamingResponse(stream_direct(), status_code=206, headers=headers)
        else:
            return StreamingResponse(stream_direct(), status_code=200, headers=headers)

    except Exception as e:
        log(f"❌ DOWNLOAD ERROR: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# ============================================================
# INDEXING & UPLOAD API
# ============================================================
def verify_key(key: str):
    if key != INTERNAL_API_KEY: raise HTTPException(status_code=403, detail="Forbidden")

@app.get("/api/index_forwarded")
async def index_forwarded(key: str, message_id: int, filename: str):
    verify_key(key)
    try:
        client  = await get_client()
        message = await client.get_messages(CHANNEL_ID, ids=message_id)
        if not message or not message.document: return {"error": "Not Found"}
        
        doc = message.document
        short_id = str(uuid.uuid4())[:8]
        save_file_entry(short_id, {
            "message_id": message.id, "filename": filename, "size": getattr(doc, 'size', 0),
            "content_type": getattr(message.file, 'mime_type', "application/octet-stream"), "channel_id": CHANNEL_ID,
            "doc_id": doc.id, "access_hash": doc.access_hash,
            "file_reference": doc.file_reference.hex(), "dc_id": doc.dc_id,
        })
        return [{"file_code": short_id, "file_status": "OK"}]
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/upload")
async def mock_upload(key: str, file_0: UploadFile = File(...)):
    verify_key(key)
    filename = file_0.filename
    tmp_path = f"/tmp/{uuid.uuid4()}{Path(filename).suffix}"
    
    try:
        with open(tmp_path, "wb") as f:
            while chunk := await file_0.read(4 * 1024 * 1024):
                f.write(chunk)

        client = await get_client()
        uploaded_file = await parallel_upload(client, tmp_path)
        message = await client.send_file(CHANNEL_ID, uploaded_file, force_document=True)
        
        short_id = str(uuid.uuid4())[:8]
        save_file_entry(short_id, {
            "message_id": message.id, "filename": filename, "size": os.path.getsize(tmp_path),
            "content_type": file_0.content_type, "channel_id": CHANNEL_ID,
            "doc_id": message.document.id, "access_hash": message.document.access_hash,
            "file_reference": message.document.file_reference.hex(), "dc_id": message.document.dc_id,
        })
        return [{"file_code": short_id, "file_status": "OK"}]
    finally:
        if os.path.exists(tmp_path): os.unlink(tmp_path)

@app.get("/files")
async def list_files(page: int = 1, limit: int = 10, key: str = ""):
    verify_key(key)
    conn   = get_db_connection()
    offset = (page - 1) * limit
    rows   = conn.execute("SELECT * FROM files ORDER BY rowid DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    conn.close()
    return {"files": [dict(r) for r in rows]}
