import os
import uuid
import tempfile
import asyncio
import time
import sqlite3
import threading
import math
from pathlib import Path
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
# TELEGRAM CLIENT
# ============================================================
_client = None

async def get_client():
    global _client
    if _client and _client.is_connected():
        return _client
    session = StringSession(SESSION_STR) if SESSION_STR else StringSession()
    _client = TelegramClient(
        session, API_ID, API_HASH,
        connection_retries=10,
        retry_delay=2,
        request_retries=5,
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
    conn.execute("PRAGMA cache_size=10000")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.commit()
    conn.close()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE_SQLITE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
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

# ============================================================
# STARTUP
# ============================================================
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
# PARALLEL HTTP DOWNLOADER (remote upload ke liye)
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
# ⚡ PARALLEL UPLOAD — 15 concurrent parts via SaveBigFilePartRequest
# workers= parameter nahi — sab Telethon versions ke saath compatible
# Yahi original speed deta tha 7-8 MB/s
# ============================================================
async def parallel_upload(client, file_path):
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    log(f"⬆️ Parallel uploading {format_size(file_size)}...")

    # Small files: direct upload, no overhead
    if file_size < 10 * 1024 * 1024:
        return await client.upload_file(file_path, part_size_kb=512)

    part_size   = 512 * 1024   # 512KB — Telegram max part size
    total_parts = math.ceil(file_size / part_size)
    file_id     = int.from_bytes(os.urandom(8), "big", signed=True)

    # 15 concurrent parts = fast but flood-safe
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
                    log(f"  Part {part_idx} attempt {attempt+1} failed: {ex}")
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
    if not entry:
        raise HTTPException(status_code=404, detail="File not found")
    filename_safe = entry["filename"].replace('"', '')
    return Response(
        status_code=200,
        headers={
            "Content-Length": str(int(entry["size"])),
            "Content-Type": entry["content_type"] or "application/octet-stream",
            "Accept-Ranges": "bytes",
            "Content-Disposition": f'attachment; filename="{filename_safe}"',
        }
    )

# ============================================================
# ⚡ DOWNLOAD — 16MB chunks (was 4MB = 4x slower)
# ============================================================
@app.get("/download/{short_id}")
async def download_file(short_id: str):
    entry = get_file_entry(short_id)
    if not entry:
        raise HTTPException(status_code=404, detail="File not found")

    file_size     = int(entry["size"])
    filename_safe = entry["filename"].replace('"', '')
    content_type  = entry["content_type"] or "application/octet-stream"

    log(f"⬇️ DOWNLOAD | {entry['filename']} | {format_size(file_size)}")

    try:
        client  = await get_client()
        message = await client.get_messages(entry["channel_id"], ids=entry["message_id"])
        if not message or not message.document:
            delete_file_entry(short_id)
            raise HTTPException(status_code=404, detail="File deleted from Telegram")
        document = message.document
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    async def stream_direct():
        try:
            async for chunk in client.iter_download(
                document,
                request_size=16 * 1024 * 1024,  # ⚡ 16MB — was 4MB, 4x speed boost
            ):
                yield bytes(chunk)
        except Exception as e:
            log(f"Stream error: {e}")

    return StreamingResponse(
        stream_direct(),
        headers={
            "Content-Disposition": f'attachment; filename="{filename_safe}"',
            "Content-Type": content_type,
            "Content-Length": str(file_size),
            "Accept-Ranges": "bytes",
            "X-Accel-Buffering": "no",
            "X-Content-Type-Options": "nosniff",
        },
        media_type=content_type
    )

# ============================================================
# OTHER ROUTES
# ============================================================
def verify_key(key: str):
    if key != INTERNAL_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")

@app.get("/files")
async def list_files(page: int = 1, limit: int = 10, key: str = ""):
    if key != INTERNAL_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid Password")
    conn        = get_db_connection()
    offset      = (page - 1) * limit
    total_row   = conn.execute("SELECT COUNT(*) as count FROM files").fetchone()
    total_count = total_row["count"]
    rows        = conn.execute("SELECT short_id, filename, size FROM files ORDER BY rowid DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    conn.close()
    total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
    return {
        "files": [{"short_id": r["short_id"], "filename": r["filename"], "size": format_size(r["size"]), "download_link": f"{BASE_URL}/download/{r['short_id']}"} for r in rows],
        "total": total_count, "page": page, "total_pages": total_pages, "limit": limit
    }

@app.get("/info/{short_id}")
async def file_info(short_id: str):
    entry = get_file_entry(short_id)
    if not entry:
        raise HTTPException(status_code=404, detail="File not found")
    return {"filename": entry["filename"], "size": format_size(entry["size"]), "content_type": entry["content_type"], "download_link": f"{BASE_URL}/download/{short_id}"}

@app.get("/sync")
async def sync_files():
    return {"status": "Sync managed automatically.", "removed": 0, "remaining": "all"}

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

# ============================================================
# ⚡ NEW: INSTANT FORWARD INDEXING API (0 Seconds)
# ============================================================
@app.get("/api/index_forwarded")
async def index_forwarded(key: str, message_id: int, filename: str):
    verify_key(key)
    try:
        client = await get_client()
        # Fetch the specific message from the channel using the ID from the Node.js bot
        message = await client.get_messages(CHANNEL_ID, ids=message_id)
        
        if not message or not message.document:
            log(f"❌ INDEX ERROR: Message or document not found for ID {message_id}")
            return {"error": "Message or Document not found in channel"}
            
        doc = message.document
        file_size = getattr(doc, 'size', 0)
        
        # Determine content type (fallback to octet-stream if unknown)
        content_type = getattr(message.file, 'mime_type', "application/octet-stream")
        
        short_id = str(uuid.uuid4())[:8]
        
        # Save directly to the database without downloading
        save_file_entry(short_id, {
            "message_id": message.id, 
            "filename": filename, 
            "size": file_size,
            "content_type": content_type,
            "channel_id": CHANNEL_ID,
            "doc_id": doc.id,
            "access_hash": doc.access_hash,
            "file_reference": doc.file_reference.hex(),
            "dc_id": doc.dc_id,
        })
        
        log(f"✅ INSTANT INDEX DONE | {short_id} | {filename}")
        return [{"file_code": short_id, "file_status": "OK"}]
        
    except Exception as e:
        log(f"❌ INDEX ERROR: {e}")
        return {"error": str(e)}

@app.post("/api/upload")
async def mock_upload(key: str, file_0: UploadFile = File(...)):
    verify_key(key)
    filename = file_0.filename
    suffix   = Path(filename).suffix or ".bin"
    tmp      = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp_path = tmp.name
    tmp.close()

    try:
        def write_sync(chunk):
            with open(tmp_path, 'ab') as f: f.write(chunk)
        while True:
            chunk = await file_0.read(4 * 1024 * 1024)
            if not chunk: break
            await asyncio.to_thread(write_sync, chunk)

        file_size = os.path.getsize(tmp_path)
        client    = await get_client()
        log(f"⬆️ UPLOAD | {filename} | {format_size(file_size)}")

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
            "content_type": file_0.content_type or "application/octet-stream",
            "channel_id": CHANNEL_ID,
            "doc_id": doc.id if doc else None,
            "access_hash": doc.access_hash if doc else None,
            "file_reference": doc.file_reference.hex() if doc else None,
            "dc_id": doc.dc_id if doc else None,
        })
        log(f"✅ UPLOAD DONE | {short_id}")
        return [{"file_code": short_id, "file_status": "OK"}]
    except Exception as e:
        log(f"❌ UPLOAD ERROR: {e}")
        return {"error": str(e)}
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
