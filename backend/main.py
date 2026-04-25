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
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
import sys
import aiohttp
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import InputFileBig
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
# TELEGRAM CLIENT (Optimized for 16 Concurrent Threads)
# ============================================================
_client = None

async def get_client():
    global _client
    if _client and _client.is_connected():
        return _client
    session = StringSession(SESSION_STR) if SESSION_STR else StringSession()
    _client = TelegramClient(
        session, API_ID, API_HASH,
        connection_retries=15,
        retry_delay=2,
        request_retries=10,
        flood_sleep_threshold=15,
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
# DATABASE — WAL mode
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
    log("🚀 TeleStore Started!")

@app.get("/", response_class=HTMLResponse)
async def root():
    index = FRONTEND_DIR / "index.html"
    if index.exists(): return HTMLResponse(content=index.read_text())
    return HTMLResponse(content="<h1>TeleStore Running</h1>")

@app.get("/favicon.ico")
async def favicon(): return HTMLResponse("")

# ============================================================
# PARALLEL HTTP DOWNLOADER (For Remote Uploads)
# ============================================================
async def fast_http_download(url: str, output_file: str, max_workers: int = 15):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={'Range': 'bytes=0-0'}) as test_res:
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

        async def download_chunk(s, e):
            async with sem:
                for _ in range(5):
                    try:
                        async with session.get(url, headers={'Range': f'bytes={s}-{e}'}, timeout=aiohttp.ClientTimeout(total=60)) as res:
                            data = await res.read()
                            def write_chunk():
                                with open(output_file, 'r+b') as f:
                                    f.seek(s); f.write(data)
                            await asyncio.to_thread(write_chunk)
                            return
                    except: await asyncio.sleep(1)
                raise Exception(f"Failed chunk {s}-{e}")

        await asyncio.gather(*[asyncio.create_task(download_chunk(s, e)) for s, e in ranges])
        return total_size

# ============================================================
# ⚡ PARALLEL UPLOAD
# ============================================================
async def parallel_upload(client, file_path):
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    log(f"⬆️ Parallel uploading {format_size(file_size)}...")

    if file_size < 10 * 1024 * 1024:
        return await client.upload_file(file_path, part_size_kb=512)

    part_size   = 512 * 1024
    total_parts = math.ceil(file_size / part_size)
    file_id     = int.from_bytes(os.urandom(8), "big", signed=True)

    sem = asyncio.Semaphore(15)

    async def upload_part(part_idx):
        async with sem:
            start = part_idx * part_size
            end   = min(start + part_size, file_size)
            with open(file_path, 'rb') as f:
                f.seek(start)
                chunk = f.read(end - start)
            for attempt in range(5):
                try:
                    await client(SaveBigFilePartRequest(file_id, part_idx, total_parts, chunk))
                    return
                except Exception as ex:
                    await asyncio.sleep(1 * (attempt + 1))
            raise Exception(f"Part {part_idx} failed after 5 attempts")

    tasks = [asyncio.create_task(upload_part(i)) for i in range(total_parts)]
    await asyncio.gather(*tasks)
    log(f"✅ All {total_parts} parts uploaded")
    return InputFileBig(id=file_id, parts=total_parts, name=file_name)

# ============================================================
# HEAD — size check
# ============================================================
@app.head("/download/{short_id}")
async def download_head(short_id: str):
    entry = get_file_entry(short_id)
    if not entry: raise HTTPException(status_code=404)
    filename_safe = quote(entry["filename"])
    return Response(
        status_code=200,
        headers={
            "Content-Length": str(int(entry["size"])),
            "Content-Type": entry["content_type"] or "application/octet-stream",
            "Accept-Ranges": "bytes",
            "Content-Disposition": f"attachment; filename*=UTF-8''{filename_safe}",
        }
    )

# ============================================================
# 🚀🔥 THE HACKER ENGINE: 16-PIPE SERVER-SIDE ADM DOWNLOADER
# ============================================================
@app.get("/download/{short_id}")
async def download_file(request: Request, short_id: str):
    entry = get_file_entry(short_id)
    if not entry:
        raise HTTPException(status_code=404, detail="File not found")

    file_size = int(entry["size"])
    filename_raw = entry["filename"]
    content_type = entry["content_type"] or "application/octet-stream"

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

    if start_byte >= file_size:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    content_length = end_byte - start_byte + 1
    log(f"⬇️ FAST DOWNLOAD (16 Pipes) | {filename_raw} | {format_size(file_size)}")

    try:
        client = await get_client()
        message = await client.get_messages(entry["channel_id"], ids=entry["message_id"])
        if not message or not message.document:
            raise HTTPException(status_code=404, detail="File deleted from Telegram")
        
        document = message.document

        async def stream_direct():
            # 🔥 16-PIPE ENGINE LOGIC 🔥
            chunk_size = 1 * 1024 * 1024  # 1MB ke tukde (RAM aur Caddy safe)
            prefetch_tasks = 16  # 👈 16 Parallel Pipes direct hit to Telegram

            async def download_exact_chunk(off, length):
                data = b""
                try:
                    async for chunk in client.iter_download(document, offset=off, request_size=1024*1024):
                        data += chunk
                        if len(data) >= length:
                            return data[:length]
                except Exception as e:
                    log(f"Chunk error at {off}: {e}")
                return data

            try:
                current_offset = start_byte
                pending_tasks = []
                
                while current_offset <= end_byte or pending_tasks:
                    # 1. Background mein 16 pipe lagao
                    while len(pending_tasks) < prefetch_tasks and current_offset <= end_byte:
                        length = min(chunk_size, end_byte - current_offset + 1)
                        task = asyncio.create_task(download_exact_chunk(current_offset, length))
                        pending_tasks.append(task)
                        current_offset += length
                    
                    # 2. Jaise hi data ready ho, Mobile ko phenk do
                    if pending_tasks:
                        first_task = pending_tasks.pop(0)
                        chunk_data = await first_task
                        
                        if not chunk_data:
                            break
                            
                        # Micro-chunking: Data pass karte waqt server hang na ho
                        step = 256 * 1024
                        for i in range(0, len(chunk_data), step):
                            yield chunk_data[i:i+step]
                            await asyncio.sleep(0.001) # Breathe for event loop
                            
            except asyncio.CancelledError:
                # Browser closed connection
                pass
            except Exception as e:
                log(f"Parallel Stream Error: {e}")

        encoded_filename = quote(filename_raw)
        
        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}",
            "Content-Type": content_type,
            "Content-Length": str(content_length),
            "Accept-Ranges": "bytes",
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        }
        
        status_code = 206 if range_header else 200
        if range_header:
            headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{file_size}"
            
        return StreamingResponse(stream_direct(), status_code=status_code, headers=headers, media_type=content_type)

    except HTTPException:
        raise
    except Exception as e:
        log(f"❌ DOWNLOAD ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# OTHER ROUTES
# ============================================================
def verify_key(key: str):
    if key != INTERNAL_API_KEY: raise HTTPException(status_code=403)

@app.get("/files")
async def list_files(page: int = 1, limit: int = 10, key: str = ""):
    verify_key(key)
    conn   = get_db_connection()
    offset = (page - 1) * limit
    total_row   = conn.execute("SELECT COUNT(*) as count FROM files").fetchone()
    total_count = total_row["count"]
    rows   = conn.execute("SELECT short_id, filename, size FROM files ORDER BY rowid DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    conn.close()
    return {
        "files": [{"short_id": r["short_id"], "filename": r["filename"], "size": format_size(r["size"]), "download_link": f"{BASE_URL}/download/{r['short_id']}"} for r in rows],
        "total": total_count, "page": page, "total_pages": math.ceil(total_count / limit) if total_count > 0 else 1, "limit": limit
    }

@app.get("/api/index_forwarded")
async def index_forwarded(key: str, message_id: int, filename: str):
    verify_key(key)
    try:
        client = await get_client()
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

@app.post("/api/remote_upload")
async def mock_remote_upload(request: Request):
    tmp_path = None
    try:
        data     = await request.json()
        key      = data.get("key")
        url      = data.get("url")
        filename = data.get("filename", f"file_{int(time.time())}.bin")
        verify_key(key)

        suffix   = Path(filename).suffix or ".bin"
        tmp      = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp_path = tmp.name
        tmp.close()

        log(f"📥 REMOTE | {filename}")
        file_size = await fast_http_download(url, tmp_path, max_workers=15)

        client        = await get_client()
        uploaded_file = await parallel_upload(client, tmp_path)
        message = await client.send_file(
            CHANNEL_ID, uploaded_file,
            caption=f"📁 {filename}\n💾 {format_size(file_size)}",
            force_document=True
        )
        doc      = message.document
        short_id = str(uuid.uuid4())[:8]
        save_file_entry(short_id, {
            "message_id": message.id, "filename": filename, "size": file_size,
            "content_type": "application/octet-stream",
            "channel_id": CHANNEL_ID,
            "doc_id": doc.id if doc else None,
            "access_hash": doc.access_hash if doc else None,
            "file_reference": doc.file_reference.hex() if doc else None,
            "dc_id": doc.dc_id if doc else None,
        })
        log(f"✅ REMOTE DONE | {short_id}")
        return [{"file_code": short_id, "file_status": "OK"}]
    except Exception as e:
        log(f"❌ REMOTE ERROR: {e}")
        return {"error": str(e)}
    finally:
        if tmp_path and os.path.exists(tmp_path): os.unlink(tmp_path)

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
