import os
import uuid
import tempfile
import asyncio
import time
import sqlite3
import math
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import sys

# 🟢 FAST DOWNLOAD LIBRARIES
import aiohttp 

from telethon import TelegramClient
from telethon.sessions import StringSession
# 🟢 TELEGRAM PARALLEL UPLOAD IMPORTS
from telethon.tl.types import InputFileBig, InputFile
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest

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

app = FastAPI(title="TeleStore API & DevUploads Wrapper")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🛑 TELEGRAM CONFIG 🛑
BOT_TOKEN   = os.getenv("BOT_TOKEN", "")
API_ID      = int(os.getenv("API_ID", "0"))
API_HASH    = os.getenv("API_HASH", "")
CHANNEL_ID  = int(os.getenv("CHANNEL_ID", "0"))
BASE_URL    = os.getenv("BASE_URL", "http://127.0.0.1:9500")
SESSION_STR = os.getenv("SESSION_STRING", "")

DB_FILE_SQLITE = "/app/data/files.db"
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "super_secret_key_123")

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

_client = None

async def get_client():
    global _client
    if _client is None or not _client.is_connected():
        session = StringSession(SESSION_STR) if SESSION_STR else StringSession()
        _client = TelegramClient(session, API_ID, API_HASH, connection_retries=5)
        await _client.start(bot_token=BOT_TOKEN)
    return _client

def format_size(size_bytes):
    if size_bytes == 0: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

# ========================================================
# ⚡ SUPERFAST SQLITE DATABASE HELPERS
# ========================================================
def init_db():
    os.makedirs(os.path.dirname(DB_FILE_SQLITE), exist_ok=True)
    conn = sqlite3.connect(DB_FILE_SQLITE)
    conn.execute('''CREATE TABLE IF NOT EXISTS files (
        short_id TEXT PRIMARY KEY,
        message_id INTEGER,
        filename TEXT,
        size INTEGER,
        content_type TEXT,
        channel_id INTEGER,
        doc_id TEXT,
        access_hash TEXT,
        file_reference TEXT,
        dc_id INTEGER
    )''')
    conn.commit()
    conn.close()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE_SQLITE)
    conn.row_factory = sqlite3.Row
    return conn

def get_file_entry(short_id):
    conn = get_db_connection()
    row = conn.execute("SELECT * FROM files WHERE short_id = ?", (short_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def save_file_entry(short_id, data):
    conn = get_db_connection()
    conn.execute('''REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (short_id, data.get("message_id"), data.get("filename"), data.get("size"),
         data.get("content_type"), data.get("channel_id"), str(data.get("doc_id")),
         str(data.get("access_hash")), str(data.get("file_reference")), data.get("dc_id")))
    conn.commit()
    conn.close()

def delete_file_entry(short_id):
    conn = get_db_connection()
    conn.execute("DELETE FROM files WHERE short_id = ?", (short_id,))
    conn.commit()
    conn.close()

@app.on_event("startup")
async def startup_event():
    init_db()
    log("🚀 Server Starting Fast with Parallel Engine & Perfect Download!")

@app.get("/", response_class=HTMLResponse)
async def root():
    index = FRONTEND_DIR / "index.html"
    if index.exists(): return HTMLResponse(content=index.read_text())
    return HTMLResponse(content="<h1>TeleStore API Running Fast 🚀</h1>")

@app.get("/favicon.ico")
async def favicon(): return HTMLResponse("")

# ========================================================
# 🔥 IDM-STYLE PARALLEL DOWNLOADER (Cloud to Server)
# ========================================================
async def fast_http_download(url: str, output_file: str, max_workers: int = 15):
    async with aiohttp.ClientSession() as session:
        headers = {'Range': 'bytes=0-0'}
        async with session.get(url, headers=headers) as test_res:
            is_range_supported = test_res.status == 206
            total_size = 0
            if is_range_supported:
                cr = test_res.headers.get('Content-Range', '')
                if '/' in cr: total_size = int(cr.split('/')[1])
            elif test_res.status == 200:
                total_size = int(test_res.headers.get('Content-Length', 0))

        if not is_range_supported or total_size == 0:
            async with session.get(url) as res:
                if res.status != 200: raise Exception(f"HTTP Error {res.status}")
                with open(output_file, 'wb') as f:
                    async for chunk in res.content.iter_chunked(5 * 1024 * 1024):
                        f.write(chunk)
            return os.path.getsize(output_file)

        with open(output_file, "wb") as f:
            f.truncate(total_size)

        chunk_size = 5 * 1024 * 1024
        ranges = [(i, min(i + chunk_size - 1, total_size - 1)) for i in range(0, total_size, chunk_size)]
        sem = asyncio.Semaphore(max_workers)

        async def download_chunk(start, end):
            async with sem:
                for attempt in range(5):
                    try:
                        async with session.get(url, headers={'Range': f'bytes={start}-{end}'}, timeout=30) as res:
                            data = await res.read()
                            def write_to_disk():
                                with open(output_file, 'r+b') as f:
                                    f.seek(start)
                                    f.write(data)
                            await asyncio.to_thread(write_to_disk)
                            return
                    except Exception:
                        await asyncio.sleep(1)
                raise Exception(f"Failed chunk {start}-{end}")

        tasks = [asyncio.create_task(download_chunk(s, e)) for s, e in ranges]
        await asyncio.gather(*tasks)
        return total_size

# ========================================================
# 🚀🚀 MULTI-THREADED TELEGRAM UPLOADER (Server to TG)
# ========================================================
async def parallel_upload(client, file_path):
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    
    if file_size < 10 * 1024 * 1024:
        return await client.upload_file(file_path)
        
    part_size = 512 * 1024 
    total_parts = math.ceil(file_size / part_size)
    file_id = int.from_bytes(os.urandom(8), "big", signed=True) 
    is_big = file_size > 10 * 1024 * 1024
    
    sem = asyncio.Semaphore(15) 
    
    async def upload_part(part_idx, chunk_data):
        async with sem:
            for attempt in range(5):
                try:
                    if is_big:
                        await client(SaveBigFilePartRequest(file_id, part_idx, total_parts, chunk_data))
                    else:
                        await client(SaveFilePartRequest(file_id, part_idx, chunk_data))
                    return
                except Exception as e:
                    await asyncio.sleep(1)
            raise Exception(f"Failed to upload part {part_idx}")

    tasks = []
    with open(file_path, 'rb') as f:
        for i in range(total_parts):
            chunk = f.read(part_size)
            tasks.append(asyncio.create_task(upload_part(i, chunk)))

    await asyncio.gather(*tasks)
    
    if is_big:
        return InputFileBig(id=file_id, parts=total_parts, name=file_name)
    else:
        return InputFile(id=file_id, parts=total_parts, name=file_name, md5_checksum="")

# ========================================================
# 📂 🟢 ORIGINAL FAST DOWNLOAD ROUTE (Size Fix + Speed Fix)
# ========================================================
# ========================================================
# 📂 🟢 100% PERFECT DOWNLOAD ROUTE (Size Fix + IDM Multi-Thread Speed)
# ========================================================
# ========================================================
# 📂 🟢 100% PERFECT DOWNLOAD ROUTE (Fixes '?' Size & Speed)
# ========================================================
@app.get("/download/{short_id}")
async def download_file(short_id: str, request: Request):
    entry = get_file_entry(short_id)
    if not entry: raise HTTPException(status_code=404, detail="File not found")

    file_size = int(entry["size"])
    t_start = time.time()
    log(f"⬇️  DOWNLOAD START | {entry['filename']} | {file_size/(1024*1024):.1f}MB")

    try:
        client = await get_client()
        message = await client.get_messages(entry["channel_id"], ids=entry["message_id"])

        if not message or not message.document:
            delete_file_entry(short_id)
            raise HTTPException(status_code=404, detail="File deleted from Telegram")

        document = message.document
    except Exception as e: 
        raise HTTPException(status_code=500, detail=str(e))

    # 🟢 MAGIC: Handling Browser/IDM Range Requests for Exact Size
    range_header = request.headers.get("Range")
    start = 0
    end = file_size - 1

    if range_header:
        try:
            ranges = range_header.replace("bytes=", "").split("-")
            start = int(ranges[0]) if ranges[0] else 0
            end = int(ranges[1]) if len(ranges) > 1 and ranges[1] else file_size - 1
        except:
            pass

    if start >= file_size or end >= file_size:
        return StreamingResponse(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    limit = end - start + 1
    filename_safe = entry["filename"].replace('"', '')

    async def stream_from_telegram():
        # Telegram API chunk size max 1MB rakha hai for consistent speed
        async for chunk in client.iter_download(document, offset=start, limit=limit, request_size=1024 * 1024):
            yield bytes(chunk)

    # 🟢 ANTI-PROXY HEADERS (Ab size hamesha dikhega)
    headers = {
        "Content-Disposition": f'attachment; filename="{filename_safe}"',
        "Content-Type": entry["content_type"] or "application/octet-stream",
        "Content-Length": str(limit),  # Force exact size
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store, no-cache, must-revalidate", # Cloudflare ko data cache karne se rokega
        "X-Accel-Buffering": "no", # Nginx ko stream rokne se rokega
        "Connection": "keep-alive"
    }

    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        return StreamingResponse(stream_from_telegram(), status_code=206, headers=headers)
    else:
        return StreamingResponse(stream_from_telegram(), status_code=200, headers=headers)


# ========================================================
# 📂 OTHER API ROUTES
# ========================================================
def verify_key(key: str):
    if key != INTERNAL_API_KEY: raise HTTPException(status_code=403, detail="Invalid API Key")

@app.get("/files")
async def list_files():
    conn = get_db_connection()
    rows = conn.execute("SELECT short_id, filename, size FROM files ORDER BY rowid DESC LIMIT 100").fetchall()
    conn.close()
    files = [{"short_id": r["short_id"], "filename": r["filename"], "size": format_size(r["size"]), "download_link": f"{BASE_URL}/download/{r['short_id']}"} for r in rows]
    return {"files": files, "total": len(files)}

@app.get("/info/{short_id}")
async def file_info(short_id: str):
    entry = get_file_entry(short_id)
    if not entry: raise HTTPException(status_code=404, detail="File not found")
    return {"filename": entry["filename"], "size": format_size(entry["size"]), "content_type": entry["content_type"], "download_link": f"{BASE_URL}/download/{short_id}"}

@app.get("/sync")
async def sync_files(): return {"status": "Sync is managed automatically.", "removed": 0, "remaining": "all"}

@app.get("/api/file/info")
async def mock_file_info(key: str, file_code: str):
    verify_key(key)
    entry = get_file_entry(file_code)
    if not entry: return {"status": 404, "msg": "File not found"}
    return {"status": 200, "result": [{"file_code": file_code, "name": entry["filename"], "file_name": entry["filename"], "size": str(entry["size"]), "file_size": str(entry["size"])}]}

@app.get("/api/upload/server")
async def mock_upload_server(key: str):
    verify_key(key)
    return {"status": 200, "result": f"{BASE_URL}/api/upload?key={key}", "sess_id": "telegram_session"}

@app.post("/api/upload")
async def mock_upload(key: str, file_0: UploadFile = File(...)):
    verify_key(key)
    filename = file_0.filename
    
    suffix = Path(filename).suffix or ".bin"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp_path = tmp.name
    tmp.close()

    try:
        def write_sync(chunk):
            with open(tmp_path, 'ab') as f: f.write(chunk)
            
        while True:
            chunk = await file_0.read(10 * 1024 * 1024) 
            if not chunk: break
            await asyncio.to_thread(write_sync, chunk)
                
        file_size = os.path.getsize(tmp_path)
        client = await get_client()
        log(f"⚡ FAST PARALLEL UPLOAD TO TG START | {filename}")
        
        uploaded_file = await parallel_upload(client, tmp_path)
        
        message = await client.send_file(CHANNEL_ID, uploaded_file, caption=f"📁 {filename}\n💾 {format_size(file_size)}", force_document=True)
        doc = message.document
        short_id = str(uuid.uuid4())[:8]
        
        save_file_entry(short_id, {
            "message_id": message.id, "filename": filename, "size": file_size,
            "content_type": file_0.content_type or "application/octet-stream",
            "channel_id": CHANNEL_ID, "doc_id": doc.id if doc else None,
            "access_hash": doc.access_hash if doc else None,
            "file_reference": doc.file_reference.hex() if doc else None,
            "dc_id": doc.dc_id if doc else None,
        })
        return [{"file_code": short_id, "file_status": "OK"}]
    except Exception as e:
        log(f"❌ API UPLOAD ERROR: {e}")
        return {"error": str(e)}
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

@app.post("/api/remote_upload")
async def mock_remote_upload(request: Request):
    try:
        data = await request.json()
        key = data.get("key")
        url = data.get("url")
        filename = data.get("filename", f"file_{int(time.time())}.bin")
        verify_key(key)
        
        suffix = Path(filename).suffix or ".bin"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp_path = tmp.name
        tmp.close() 

        log(f"📥 PYTHON IDM DOWNLOADING | {filename}")
        file_size = await fast_http_download(url, tmp_path, max_workers=15)
        
        client = await get_client()
        log(f"⚡ FAST PARALLEL UPLOADING | {filename} ({format_size(file_size)})")
        
        uploaded_file = await parallel_upload(client, tmp_path)
        
        message = await client.send_file(
            CHANNEL_ID, uploaded_file,
            caption=f"📁 {filename}\n💾 {format_size(file_size)}", force_document=True
        )
        
        doc = message.document
        short_id = str(uuid.uuid4())[:8]
        
        save_file_entry(short_id, {
            "message_id": message.id, "filename": filename, "size": file_size,
            "content_type": "application/octet-stream",
            "channel_id": CHANNEL_ID, "doc_id": doc.id if doc else None,
            "access_hash": doc.access_hash if doc else None,
            "file_reference": doc.file_reference.hex() if doc else None,
            "dc_id": doc.dc_id if doc else None,
        })
        
        log(f"✅ PYTHON PROCESS DONE | ID: {short_id}")
        return [{"file_code": short_id, "file_status": "OK"}]
    
    except Exception as e:
        log(f"❌ REMOTE UPLOAD ERROR: {e}")
        return {"error": str(e)}
    finally:
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            os.unlink(tmp_path)

@app.get("/api/file/clone")
async def mock_clone(key: str, file_code: str):
    verify_key(key)
    entry = get_file_entry(file_code)
    if not entry: return {"status": 404, "msg": "Source file not found"}
    new_code = str(uuid.uuid4())[:8]
    save_file_entry(new_code, entry) 
    return {"status": 200, "result": {"url": f"{BASE_URL}/download/{new_code}", "filecode": new_code}}

@app.get("/api/file/rename")
async def mock_rename(key: str, file_code: str, name: str):
    verify_key(key)
    entry = get_file_entry(file_code)
    if entry:
        entry["filename"] = name
        save_file_entry(file_code, entry)
        return {"status": 200, "msg": "OK"}
    return {"status": 404, "msg": "File not found"}

@app.get("/api/file/delete")
async def mock_delete(key: str, file_code: str):
    verify_key(key)
    delete_file_entry(file_code)
    return {"status": 200, "msg": "OK"}
