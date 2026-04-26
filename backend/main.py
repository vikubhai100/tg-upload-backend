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

# Asli User ki IP nikalne ke liye (Coolify Proxy bypass)
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
# TELEGRAM CLIENTS
# _bot_client  → Upload ke liye (Bot Token)
# _user_client → Download ke liye (Session String)
#                Agar Session String nahi hai toh
#                download bhi Bot se hoga fallback mein
# ============================================================
_bot_client  = None
_user_client = None

async def get_bot_client():
    """Upload ke liye — hamesha Bot Token use karta hai"""
    global _bot_client
    if _bot_client and _bot_client.is_connected():
        return _bot_client
    _bot_client = TelegramClient(
        StringSession(), API_ID, API_HASH,
        connection_retries=15,
        retry_delay=2,
        request_retries=10,
        flood_sleep_threshold=15,
    )
    await _bot_client.start(bot_token=BOT_TOKEN)
    log("🤖 Bot Client connected (Upload ready)")
    return _bot_client

async def get_user_client():
    """Download ke liye — Session String use karta hai"""
    global _user_client
    if _user_client and _user_client.is_connected():
        return _user_client
    if not SESSION_STR:
        log("⚠️ SESSION_STRING nahi hai — Download bhi Bot se hoga")
        return await get_bot_client()
    _user_client = TelegramClient(
        StringSession(SESSION_STR), API_ID, API_HASH,
        connection_retries=15,
        retry_delay=2,
        request_retries=10,
        flood_sleep_threshold=15,
    )
    await _user_client.start()
    log("👤 User Client connected (Download turbo ready)")
    return _user_client

# Backward compat — purane calls ke liye
async def get_client():
    return await get_bot_client()

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

# ⚡ YAHAN UPDATE KIYA HAI ⚡
@app.on_event("startup")
async def startup_event():
    init_db()
    try:
        # Bot client — upload ke liye
        bot = await get_bot_client()
        await bot.get_dialogs()
        log("🤖 Bot Client ready!")
    except Exception as e:
        log(f"⚠️ Bot Client failed: {e}")
    try:
        # User client — download ke liye
        if SESSION_STR:
            user = await get_user_client()
            await user.get_dialogs()
            log("👤 User Client ready — Turbo Download ON!")
        else:
            log("⚠️ SESSION_STRING nahi hai — Download Bot se hoga (slow)")
    except Exception as e:
        log(f"⚠️ User Client failed: {e}")
    log("🚀 TeleStore Started!")

@app.get("/", response_class=HTMLResponse)
async def root():
    index = FRONTEND_DIR / "index.html"
    if index.exists(): return HTMLResponse(content=index.read_text())
    return HTMLResponse(content="<h1>TeleStore Running</h1>")

@app.get("/favicon.ico")
async def favicon(): return HTMLResponse("")

# ============================================================
# ⚡ UPLOAD LOGIC (With Client IP & Live Speed Logs)
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
# 🚀 TURBO DOWNLOAD ENGINE v2 — Smart Adaptive Streaming
# ============================================================
#
# PROBLEMS IN OLD CODE (kyun slow tha):
#
#  1. iter_download() per chunk call hoti thi — har baar naya
#     Telegram request, high overhead, slow start
#
#  2. asyncio.sleep(0.0001) yield ke baad — completely
#     unnecessary, har 256KB pe event loop yield karta tha
#     = latency add hoti thi bina kisi faide ke
#
#  3. 256KB ke chhote pieces yield kiye jaate the — browser
#     ko baar baar chhote TCP packets milte the, slow lagta tha
#
#  4. har chunk ke liye alag iter_download() stream khulti thi
#     = Telegram se 16 alag connections, sabka overhead alag
#
#  5. get_messages() har download request pe — extra RTT
#
# FIXES IN NEW CODE:
#
#  A. Ek seedha iter_download() stream — Telegram ka native
#     sequential reader, internally already optimized hai,
#     1MB request_size = maximum throughput
#
#  B. 512KB yield size — browser buffer ke saath sync,
#     TCP window ko pura utilize karta hai
#
#  C. asyncio.sleep hata diya — zero artificial delay
#
#  D. doc object pehle se DB se reconstruct — get_messages()
#     call avoid, startup latency zero
#
#  E. Range support sahi tarike se — offset seedha
#     iter_download me pass, pehle ka data skip nahi karna padta
#
# ============================================================

# DB se stored doc metadata se seedha InputDocumentFileLocation banao
# get_messages() call avoid hoti hai — latency bachti hai
from telethon.tl.types import InputDocumentFileLocation

async def _get_document_location(entry: dict):
    """
    Pehle DB se reconstruct karne ki koshish karo.
    Agar file_reference expire ho gayi, toh get_messages se refresh karo.
    """
    try:
        doc_id      = int(entry["doc_id"])
        access_hash = int(entry["access_hash"])
        file_ref    = bytes.fromhex(entry["file_reference"])
        dc_id       = int(entry["dc_id"])
        return InputDocumentFileLocation(
            id=doc_id,
            access_hash=access_hash,
            file_reference=file_ref,
            thumb_size=""
        ), dc_id, None
    except Exception:
        return None, None, None

@app.get("/download/{short_id}")
async def download_file(request: Request, short_id: str):
    client_ip = get_client_ip(request)
    entry = get_file_entry(short_id)
    if not entry:
        raise HTTPException(status_code=404, detail="File not found")

    file_size    = int(entry["size"])
    filename_raw = entry["filename"]
    content_type = entry["content_type"] or "application/octet-stream"

    range_header = request.headers.get("Range")
    start_byte   = 0
    end_byte     = file_size - 1

    if range_header:
        try:
            range_str  = range_header.replace("bytes=", "").split("-")
            start_byte = int(range_str[0]) if range_str[0] else 0
            if len(range_str) > 1 and range_str[1]:
                end_byte = int(range_str[1])
        except Exception:
            start_byte = 0
            end_byte   = file_size - 1

    if start_byte >= file_size:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    content_length = end_byte - start_byte + 1
    log(f"⬇️ DOWNLOAD START | {filename_raw} | Client: {client_ip} | Total: {format_size(content_length)}")

    async def _fresh_location(cl, ent, sid):
        """Telegram se fresh file reference lo aur DB update karo"""
        msg = await cl.get_messages(int(ent["channel_id"]), ids=int(ent["message_id"]))
        if not msg or not msg.document:
            return None, None
        d = msg.document
        loc = InputDocumentFileLocation(
            id=d.id, access_hash=d.access_hash,
            file_reference=d.file_reference, thumb_size=""
        )
        ent["file_reference"] = d.file_reference.hex()
        ent["doc_id"]         = str(d.id)
        ent["access_hash"]    = str(d.access_hash)
        ent["dc_id"]          = d.dc_id
        save_file_entry(sid, ent)
        log(f"🔄 FILE REF REFRESHED | {ent['filename']}")
        return loc, d.dc_id

    try:
        client = await get_user_client()

        doc_location, dc_id, _ = await _get_document_location(entry)
        if doc_location is None:
            doc_location, dc_id = await _fresh_location(client, entry, short_id)
            if doc_location is None:
                raise HTTPException(status_code=404, detail="File deleted from Telegram")

        async def turbo_stream():
            nonlocal doc_location, dc_id
            # ═══════════════════════════════════════════════════
            # PARALLEL DOWNLOAD ENGINE
            # ───────────────────────────────────────────────────
            # Telegram ek single stream ko throttle karta hai
            # ~300KB/s tak. Lekin 4-5 parallel connections pe
            # throttle nahi lagta — combined speed 3-8 MB/s
            # milti hai.
            #
            # Logic:
            # 1. File ko PIPE_COUNT = 4 parallel segments mein
            #    baanto (har pipe 2MB fetch karta hai)
            # 2. Har pipe asyncio.Queue mein chunks dalta hai
            # 3. Main loop IN-ORDER yield karta hai —
            #    browser ko sequential data milta hai
            # ═══════════════════════════════════════════════════

            PIPE_COUNT   = 4          # parallel Telegram connections
            PIPE_SIZE    = 2*1024*1024 # har pipe 2MB fetch karta hai
            YIELD_SIZE   = 512*1024    # browser ko 512KB chunks
            REQUEST_SIZE = 1*1024*1024 # Telegram se 1MB per request

            start_time    = time.time()
            last_log_time = start_time
            total_sent    = 0
            loc           = doc_location
            dcid          = dc_id

            async def fetch_segment(off, length, q):
                """Ek segment fetch karke queue mein daalo"""
                fetched = b""
                retry   = 0
                while retry < 3:
                    try:
                        async for chunk in client.iter_download(
                            loc, offset=off,
                            request_size=REQUEST_SIZE, dc_id=dcid,
                        ):
                            fetched += chunk
                            if len(fetched) >= length:
                                await q.put(fetched[:length])
                                return
                        # Agar loop end ho gaya
                        if fetched:
                            await q.put(fetched[:length])
                        return
                    except Exception as e:
                        err = str(e).lower()
                        if "file reference" in err or "expired" in err:
                            nonlocal loc, dcid
                            log(f"🔄 FILE REF EXPIRED in pipe | {filename_raw}")
                            try:
                                loc, dcid = await _fresh_location(client, entry, short_id)
                                if loc is None:
                                    await q.put(None)
                                    return
                                retry += 1
                                fetched = b""
                                continue
                            except Exception:
                                await q.put(None)
                                return
                        else:
                            log(f"❌ Pipe error at {off}: {e}")
                            retry += 1
                            fetched = b""
                            await asyncio.sleep(0.5 * retry)
                await q.put(None)  # Max retries exhausted

            # ── Sliding window: PIPE_COUNT pipes ek saath ──────
            current_offset = start_byte
            end_offset     = start_byte + content_length
            pipes          = []  # (queue, expected_length)

            # Pehle PIPE_COUNT pipes start karo
            for _ in range(PIPE_COUNT):
                if current_offset >= end_offset:
                    break
                seg_len = min(PIPE_SIZE, end_offset - current_offset)
                q = asyncio.Queue(maxsize=1)
                asyncio.create_task(fetch_segment(current_offset, seg_len, q))
                pipes.append((q, seg_len))
                current_offset += seg_len

            buf = b""
            while pipes:
                q, seg_len = pipes.pop(0)
                data = await q.get()

                if data is None:
                    log(f"❌ Segment failed | {filename_raw}")
                    break

                buf        += data
                total_sent += len(data)

                # Yield 512KB chunks to browser
                while len(buf) >= YIELD_SIZE:
                    yield buf[:YIELD_SIZE]
                    buf = buf[YIELD_SIZE:]

                now = time.time()
                if now - last_log_time >= 3.0:
                    speed = total_sent / max((now - start_time), 0.1)
                    log(f"📡 STREAMING | {filename_raw} | {format_size(min(total_sent,content_length))}/{format_size(content_length)} | {format_size(speed)}/s")
                    last_log_time = now

                # Agle segment ka task launch karo (sliding window)
                if current_offset < end_offset:
                    seg_len = min(PIPE_SIZE, end_offset - current_offset)
                    nq = asyncio.Queue(maxsize=1)
                    asyncio.create_task(fetch_segment(current_offset, seg_len, nq))
                    pipes.append((nq, seg_len))
                    current_offset += seg_len

            # Bacha hua buffer bhejo
            if buf:
                yield buf

            total_time = max(time.time() - start_time, 0.1)
            log(f"✅ DONE | {filename_raw} | {format_size(min(total_sent,content_length)/total_time)}/s | {total_time:.1f}s")

        # ── Response headers ────────────────────────────────
        encoded_filename = quote(filename_raw)
        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}",
            "Content-Type"       : content_type,
            "Content-Length"     : str(content_length),
            "Accept-Ranges"      : "bytes",
            "X-Accel-Buffering"  : "no",
            "Cache-Control"      : "no-store, no-cache, must-revalidate, max-age=0",
        }

        status_code = 206 if range_header else 200
        if range_header:
            headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{file_size}"

        return StreamingResponse(
            turbo_stream(),
            status_code = status_code,
            headers     = headers,
            media_type  = content_type,
        )

    except HTTPException:
        raise
    except Exception as e:
        log(f"❌ DOWNLOAD ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# OTHER ROUTES (INTACT)
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

        client = await get_client()
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
        client        = await get_client()
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
