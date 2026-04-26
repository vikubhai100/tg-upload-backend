import os
import uuid
import tempfile
import asyncio
import time
import sqlite3
import threading
import math
import random
from pathlib import Path
from urllib.parse import quote
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
import sys
import aiohttp
from telethon import TelegramClient
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

def get_client_ip(request: Request):
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded: return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "Unknown-IP"

app = FastAPI(title="TeleStore API (Multi-Bot Optimized)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# ⚡ SUPER SMART TOKEN LOADER (Reads BOT_TOKEN_1, BOT_TOKEN_2, etc.)
# ============================================================
raw_tokens = []
for key, value in os.environ.items():
    if key.startswith("BOT_TOKEN"):
        parts = [t.strip() for t in value.split(",") if t.strip()]
        raw_tokens.extend(parts)

BOT_TOKENS = list(set(raw_tokens))

API_ID           = int(os.getenv("API_ID", "0"))
API_HASH         = os.getenv("API_HASH", "")
CHANNEL_ID       = int(os.getenv("CHANNEL_ID", "0"))
BASE_URL         = os.getenv("BASE_URL", "http://127.0.0.1:9500")
DB_FILE_SQLITE   = "/app/data/files.db"
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "super_secret_key_123")

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

# ============================================================
# MULTI-BOT CLIENT POOLING SYSTEM (WITH PERSISTENT SESSIONS)
# ============================================================
_clients = []
_bots_loading = True

async def init_clients():
    global _clients, _bots_loading
    
    if not BOT_TOKENS:
        log("❌ CRITICAL ERROR: No Bot Tokens found!")
        _bots_loading = False
        return

    log(f"🤖 Initializing {len(BOT_TOKENS)} Bot Clients...")
    os.makedirs("/app/data", exist_ok=True) # Ensure data folder exists for sessions
    
    for idx, token in enumerate(BOT_TOKENS):
        try:
            log(f"🔄 Starting Bot {idx + 1}...")
            
            # ⚡ FIX 1: Use persistent file sessions. No more temporary bans on restart!
            session_file = f"/app/data/bot_session_{idx}"
            
            client = TelegramClient(
                session_file, API_ID, API_HASH,
                connection_retries=15,
                retry_delay=2,
                request_retries=10,
                flood_sleep_threshold=15,
            )
            await client.start(bot_token=token)
            
            # ⚡ FIX 2: Append bot to list FIRST. Even if channel fetch fails, bot remains usable.
            _clients.append(client)
            log(f"✅ Bot {idx + 1} Connected and Ready!")
            
            # Run cache fetch in background safely
            async def cache_channel(c):
                try:
                    await c.get_entity(CHANNEL_ID)
                except Exception:
                    try:
                        await c.get_dialogs() 
                    except Exception as e:
                        log(f"⚠️ Bot Cache warning (Ignored): {e}")
            
            asyncio.create_task(cache_channel(client))
            
        except Exception as e:
            log(f"⚠️ Bot {idx + 1} completely failed to connect: {e}")
            
    _bots_loading = False
    log(f"🎯 Total Active Bots in Pool: {len(_clients)}")

def get_random_client():
    if _bots_loading:
        raise HTTPException(status_code=503, detail="Bots are connecting... Please wait 5 seconds.")
    if not _clients:
        raise HTTPException(status_code=500, detail="CRITICAL: No bots connected! Check your Coolify logs for FloodWait errors.")
    return random.choice(_clients)

def format_size(size_bytes):
    if size_bytes == 0: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0: return f"{size_bytes:.1f} {unit}"
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
    # Background boot
    asyncio.create_task(init_clients())
    log("🚀 TeleStore API Started. Bots are connecting in background...")

@app.get("/", response_class=HTMLResponse)
async def root():
    index = FRONTEND_DIR / "index.html"
    if index.exists(): return HTMLResponse(content=index.read_text())
    return HTMLResponse(content="<h1>TeleStore Running</h1>")

@app.get("/favicon.ico")
async def favicon(): return HTMLResponse("")

# ============================================================
# ⚡ UPLOAD LOGIC
# ============================================================
async def parallel_upload(client, file_path, client_ip="Server"):
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    log(f"⬆️ UPLOAD START | {file_name} | Client: {client_ip} | Size: {format_size(file_size)}")
    start_time = time.time()

    if file_size < 10 * 1024 * 1024:
        res = await client.upload_file(file_path, part_size_kb=512)
        end_time = time.time()
        speed = file_size / max((end_time - start_time), 0.1)
        log(f"✅ UPLOAD DONE | {file_name} | Client: {client_ip} | Speed: {format_size(speed)}/s")
        return res

    part_size   = 512 * 1024
    total_parts = math.ceil(file_size / part_size)
    file_id     = int.from_bytes(os.urandom(8), "big", signed=True)

    sem = asyncio.Semaphore(15)
    uploaded_bytes = 0
    lock = asyncio.Lock()
    last_log_time = time.time()

    async def upload_part(part_idx):
        nonlocal uploaded_bytes, last_log_time
        async with sem:
            start = part_idx * part_size
            end   = min(start + part_size, file_size)
            chunk_len = end - start
            with open(file_path, 'rb') as f:
                f.seek(start)
                chunk = f.read(chunk_len)
            for attempt in range(5):
                try:
                    await client(SaveBigFilePartRequest(file_id, part_idx, total_parts, chunk))
                    async with lock:
                        uploaded_bytes += chunk_len
                        now = time.time()
                        if now - last_log_time >= 3.0: 
                            speed = uploaded_bytes / max((now - start_time), 0.1)
                            log(f"📊 UPLOADING | {file_name} | Client: {client_ip} | {format_size(uploaded_bytes)}/{format_size(file_size)} | Speed: {format_size(speed)}/s")
                            last_log_time = now
                    return
                except Exception as ex:
                    await asyncio.sleep(1 * (attempt + 1))
            raise Exception(f"Part {part_idx} failed after 5 attempts")

    tasks = [asyncio.create_task(upload_part(i)) for i in range(total_parts)]
    await asyncio.gather(*tasks)

    end_time = time.time()
    final_speed = file_size / max((end_time - start_time), 0.1)
    log(f"✅ UPLOAD DONE | {file_name} | Client: {client_ip} | Avg Speed: {format_size(final_speed)}/s")

    return InputFileBig(id=file_id, parts=total_parts, name=file_name)

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
# 🚀 MULTI-BOT 16-PIPE DOWNLOAD ENGINE (100% SEAMLESS & STABLE)
# ============================================================
@app.get("/download/{short_id}")
async def download_file(request: Request, short_id: str):
    client_ip = get_client_ip(request)
    entry = get_file_entry(short_id)
    if not entry: raise HTTPException(status_code=404, detail="File not found")

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
        except:
            start_byte = 0
            end_byte = file_size - 1

    if start_byte >= file_size:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    content_length = end_byte - start_byte + 1
    log(f"⬇️ DOWNLOAD START | {filename_raw} | Client: {client_ip} | Size: {format_size(content_length)}")

    try:
        base_client = get_random_client()
        message = await base_client.get_messages(entry["channel_id"], ids=entry["message_id"])
        if not message or not message.document:
            raise HTTPException(status_code=404, detail="File deleted from Telegram")

        document = message.document

        async def stream_direct():
            chunk_size = 512 * 1024  
            max_concurrent = 16          
            
            start_time = time.time()
            sent_bytes = 0
            last_log_time = start_time

            chunks = []
            curr = start_byte
            while curr <= end_byte:
                size = min(chunk_size, end_byte - curr + 1)
                chunks.append({"offset": curr, "length": size})
                curr += size

            pending_queue = chunks.copy()
            active_tasks = set()
            downloaded_buffer = {}  
            next_chunk_index = 0

            sem = asyncio.Semaphore(max_concurrent)

            async def fetch_worker(chunk_info, index):
                for attempt in range(3):
                    async with sem:
                        worker_client = get_random_client()
                        try:
                            chunk_data = b""
                            async for part in worker_client.iter_download(
                                document, 
                                offset=chunk_info["offset"], 
                                request_size=512 * 1024
                            ):
                                chunk_data += part
                                if len(chunk_data) >= chunk_info["length"]:
                                    break
                            
                            exact_data = chunk_data[:chunk_info["length"]]
                            
                            if len(exact_data) == chunk_info["length"] or (chunk_info["offset"] + len(exact_data) == file_size):
                                return (index, exact_data)
                            else:
                                log(f"⚠️ Chunk {index} incomplete. Expected {chunk_info['length']}, got {len(exact_data)}. Retrying...")
                        except Exception as e:
                            log(f"⚠️ Chunk {index} fetch error (Attempt {attempt+1}): {e}")
                    
                    await asyncio.sleep(0.5) 
                
                return (index, b"") 

            try:
                while next_chunk_index < len(chunks):
                    while len(active_tasks) < max_concurrent and pending_queue:
                        c_info = pending_queue.pop(0)
                        c_idx = chunks.index(c_info)
                        task = asyncio.create_task(fetch_worker(c_info, c_idx))
                        active_tasks.add(task)

                    if not active_tasks: break

                    done, pending = await asyncio.wait(active_tasks, return_when=asyncio.FIRST_COMPLETED)
                    active_tasks = pending

                    for task in done:
                        idx, data = task.result()
                        downloaded_buffer[idx] = data

                    while next_chunk_index in downloaded_buffer:
                        data = downloaded_buffer.pop(next_chunk_index)
                        
                        if not data:
                            log(f"❌ CRITICAL: Chunk {next_chunk_index} failed permanently. Stream aborted.")
                            raise Exception("Download interrupted. Missing data chunk.")
                            
                        next_chunk_index += 1
                        
                        step = 64 * 1024 
                        for i in range(0, len(data), step):
                            chunk_piece = data[i:i+step]
                            yield chunk_piece
                            
                            sent_bytes += len(chunk_piece)
                            now = time.time()
                            if now - last_log_time >= 3.0:
                                speed = sent_bytes / max((now - start_time), 0.1)
                                log(f"📡 DOWNLOADING | {filename_raw[:15]}... | {format_size(sent_bytes)}/{format_size(content_length)} | Speed: {format_size(speed)}/s")
                                last_log_time = now
                                
                            await asyncio.sleep(0.0001)

            except asyncio.CancelledError:
                pass
            except Exception as e:
                log(f"Parallel Stream Error: {e}")
            finally:
                for task in active_tasks: task.cancel()

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
async def index_forwarded(request: Request, key: str, message_id: int, filename: str):
    verify_key(key)
    client_ip = get_client_ip(request)
    try:
        client = get_random_client()
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
        log(f"✅ INSTANT INDEX DONE | {short_id} | Client: {client_ip}")
        return [{"file_code": short_id, "file_status": "OK"}]
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/upload")
async def mock_upload(request: Request, key: str, file_0: UploadFile = File(...)):
    verify_key(key)
    client_ip = get_client_ip(request)
    filename = file_0.filename
    tmp_path = f"/tmp/{uuid.uuid4()}{Path(filename).suffix}"

    try:
        with open(tmp_path, "wb") as f:
            while chunk := await file_0.read(4 * 1024 * 1024):
                f.write(chunk)

        client = get_random_client()
        uploaded_file = await parallel_upload(client, tmp_path, client_ip=client_ip)
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
    client_ip = get_client_ip(request)
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

        log(f"📥 REMOTE DOWNLOAD START | {filename} | Client: {client_ip}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as res:
                with open(tmp_path, 'wb') as f:
                    async for chunk in res.content.iter_chunked(5 * 1024 * 1024):
                        f.write(chunk)

        file_size = os.path.getsize(tmp_path)
        client = get_random_client()
        uploaded_file = await parallel_upload(client, tmp_path, client_ip=client_ip)
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
