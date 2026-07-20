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

# Import modular configurations and database handlers
from backend.config import (
    R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET_NAME, STATE_FILE, LOG_FILE,
    BOT_TOKEN, API_ID, API_HASH, CHANNEL_ID, BASE_URL, SESSION_STR, DB_FILE_SQLITE,
    INTERNAL_API_KEY, BASE_DIR, FRONTEND_DIR, log, format_size, safeFile
)
from backend.database import init_db, get_db_connection, save_file_entry, get_file_entry, cache_cleanup_loop, r2_deduplication_loop
from backend.cloudflare import (
    get_active_worker, deploy_new_cloudflare_worker, workers_health_check_loop, 
    delete_cloudflare_worker_script, update_existing_cloudflare_worker_script, 
    fetch_latest_worker_script_from_github
)
from backend.bot_guard import bot_guard_middleware

r2_client = boto3.client(
    service_name='s3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

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

app.middleware("http")(bot_guard_middleware)

def calculate_hash(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192 * 1024): 
            hasher.update(chunk)
    return hasher.hexdigest()

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_file = FRONTEND_DIR / "index.html"
    if index_file.exists():
        return index_file.read_text(encoding="utf-8")
    index_alt = Path(__file__).resolve().parent / "index.html"
    if index_alt.exists():
        return index_alt.read_text(encoding="utf-8")
    return "<h2>Frontend Not Found (Check your /frontend folder)</h2>"

@app.get("/workers", response_class=HTMLResponse)
async def serve_workers_page():
    workers_file = FRONTEND_DIR / "workers.html"
    if workers_file.exists():
        return workers_file.read_text(encoding="utf-8")
    return "<h2>Workers Manager page not found.</h2>"

if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")
elif (Path(__file__).resolve().parent / "static").exists():
    app.mount("/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "static")), name="static")


# ============================================================
# 🆕 GENERATE CLOUDFLARE WORKER LINK
# ============================================================
async def generate_download_url(r2_key, filename, content_type, client_ip):
    """Generate a one-time worker download URL and return it as a dict (NOT a redirect)."""
    try:
        CUSTOM_DOMAIN = await get_active_worker()
        SECURE_SECRET = "URLKING_ANTI_SHARE_SECRET_2110"
        exp = int(time.time()) + 180  # 3 minute expiry

        safe_name = safeFile(filename)
        nonce = uuid.uuid4().hex  

        payload_data = {"k": r2_key, "n": safe_name, "e": exp, "i": client_ip, "nonce": nonce}
        payload_json = json.dumps(payload_data)
        token = base64.urlsafe_b64encode(payload_json.encode('utf-8')).decode('utf-8').rstrip('=')

        signature = hmac.new(
            SECURE_SECRET.encode('utf-8'),
            token.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        download_url = f"{CUSTOM_DOMAIN}/d?id={token}&sig={signature}"
        log(f"🔗 [URL GENERATED] File: {safe_name} | IP: {client_ip} | Worker: {CUSTOM_DOMAIN}")

        return {"download_url": download_url, "expires_in": 180}
    except Exception as e:
        log(f"⚠️ [URL GENERATION] Error: {str(e)}")
        return None

# ============================================================
# ⏳ 100% WAIT & PRE-CACHE TO R2 LOGIC
# ============================================================
_active_precache = {}

async def precache_telegram_to_r2(short_id, entry):
    """Downloads from Telegram and Uploads to R2. Returns cache key when 100% complete."""
    tmp_path = f"/tmp/precache_{short_id}.bin"
    try:
        client = await get_client()
        message = await client.get_messages(entry["channel_id"], ids=entry["message_id"])
        if not message or not message.document:
            log(f"⚠️ [PRE-CACHE] Message not found for {short_id}")
            return None

        # Download from Telegram
        async with aiofiles.open(tmp_path, "wb") as f_out:
            async for chunk in client.iter_download(message.document, request_size=1024*1024):
                await f_out.write(chunk)

        # Upload to R2
        r2_cache_key = f"cache_{short_id}_{uuid.uuid4().hex[:6]}"
        def s3_up():
            r2_client.upload_file(tmp_path, R2_BUCKET_NAME, r2_cache_key,
                ExtraArgs={'ContentType': entry.get("content_type") or "application/octet-stream"})
        await asyncio.to_thread(s3_up)

        # Update DB
        def update_cache_db():
            conn = get_db_connection()
            conn.execute("UPDATE files SET r2_cache_key = ? WHERE short_id = ?", (r2_cache_key, short_id))
            conn.commit()
            conn.close()
        await asyncio.to_thread(update_cache_db)

        log(f"✅ [100% UPLOAD DONE] Telegram to R2: {short_id} → {r2_cache_key}")
        return r2_cache_key
    except Exception as e:
        log(f"❌ [PRE-CACHE FAILED] Error for {short_id}: {str(e)}")
        return None
    finally:
        try: os.remove(tmp_path)
        except: pass

async def get_or_create_precache(short_id, entry):
    """Prevents multiple concurrent uploads of the same file."""
    if short_id in _active_precache:
        log(f"⏳ File {short_id} is already uploading. Waiting for it to finish 100%...")
        return await _active_precache[short_id]
    
    task = asyncio.create_task(precache_telegram_to_r2(short_id, entry))
    _active_precache[short_id] = task
    try:
        return await task
    finally:
        _active_precache.pop(short_id, None)

# ============================================================
# 🆕 INTERNAL API: URLKING FRONTEND CALLS THIS SECURELY
# ============================================================
@app.post("/api/internal/generate-download-url")
async def internal_generate_download_url(request: Request):
    """
    Called by Frontend secretly. Waits until file is 100% on R2 before replying!
    """
    internal_key = request.headers.get("X-Internal-Key", "")
    if internal_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

    try:
        data = await request.json()
        file_code = data.get("file_code")
        client_ip = data.get("client_ip", "0.0.0.0")

        if not file_code:
            return JSONResponse(status_code=400, content={"error": "file_code required"})

        entry = await asyncio.to_thread(get_file_entry, file_code)
        if not entry:
            return JSONResponse(status_code=404, content={"error": "File not found"})

        filename = entry.get("filename", "file")
        content_type = entry.get("content_type", "application/octet-stream")

        def update_access():
            conn = get_db_connection()
            try:
                conn.execute("UPDATE files SET last_accessed = ? WHERE short_id = ?", (int(time.time()), file_code))
                conn.commit()
            finally:
                conn.close()
        await asyncio.to_thread(update_access)

        r2_key_to_use = None

        if entry.get("storage_type") == "r2" and entry.get("r2_key"):
            r2_status = await asyncio.to_thread(check_r2_file_exists, entry["r2_key"])
            if r2_status == 'exists':
                r2_key_to_use = entry["r2_key"]

        if not r2_key_to_use and entry.get("r2_cache_key"):
            cache_status = await asyncio.to_thread(check_r2_file_exists, entry["r2_cache_key"])
            if cache_status == 'exists':
                r2_key_to_use = entry["r2_cache_key"]

        # 🔥 WAIT FOR 100% UPLOAD IF NOT ON R2
        if not r2_key_to_use:
            msg_id = entry.get("tg_backup_msg_id") or entry.get("message_id")
            if msg_id and int(msg_id) > 0:
                # Execution will stop here and WAIT until the upload finishes 100%
                r2_key_to_use = await get_or_create_precache(file_code, entry)
                
                if not r2_key_to_use:
                    return JSONResponse(status_code=500, content={"error": "Upload failed."})
            else:
                return JSONResponse(status_code=404, content={"error": "File data not found."})

        # Generate Worker Link
        result = await generate_download_url(r2_key_to_use, filename, content_type, client_ip)
        if not result:
            return JSONResponse(status_code=500, content={"error": "Worker generation failed"})

        return JSONResponse(content={
            "status": "OK",
            "download_url": result["download_url"],
            "filename": filename,
            "expires_in": result["expires_in"]
        })

    except Exception as e:
        log(f"❌ [INTERNAL API] Error: {str(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal server error"})


# ============================================================
# API ROUTES & FILE OPERATIONS
# ============================================================

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
        tmp_path = f"/tmp/{uuid.uuid4().hex}.bin"

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
        tmp_path = f"/tmp/remote_{uuid.uuid4().hex[:8]}.bin"

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

@app.get("/api/clean_r2_small_files")
async def clean_r2_small_files(key: str):
    verify_key(key)
    def run_cleanup():
        conn = get_db_connection()
        rows = conn.execute("SELECT short_id, r2_key FROM files WHERE size <= ? AND storage_type = 'r2' AND r2_key IS NOT NULL", (100 * 1024 * 1024,)).fetchall()
        count = 0
        for row in rows:
            try:
                r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=row["r2_key"])
                conn.execute("UPDATE files SET storage_type = 'telegram', r2_key = NULL WHERE short_id = ?", (row["short_id"],))
                count += 1
            except: pass
        conn.commit()
        conn.close()
        return count

    deleted_count = await asyncio.to_thread(run_cleanup)
    return {"status": "OK", "msg": f"Deleted {deleted_count} small files (<100MB) from R2 and forced to Telegram stream."}

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

@app.get("/api/workers")
async def get_workers(key: str):
    verify_key(key)
    def run_get():
        conn = get_db_connection()
        try:
            rows = conn.execute("SELECT * FROM workers").fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    return await db_thread(run_get)

@app.post("/api/workers/add")
async def add_worker(key: str, data: dict = Body(...)):
    verify_key(key)
    url = data.get("url", "").strip()
    if not url or not url.startswith("http"):
        raise HTTPException(status_code=400, detail="Invalid URL")

    def run_add():
        conn = get_db_connection()
        try:
            conn.execute("INSERT OR REPLACE INTO workers (url, status) VALUES (?, 'healthy')", (url,))
            conn.commit()
        finally:
            conn.close()
    await db_thread(run_add)
    return {"status": 200, "msg": "Worker added"}

@app.post("/api/workers/deploy")
async def deploy_worker(key: str):
    verify_key(key)
    new_url = await deploy_new_cloudflare_worker()
    if not new_url:
        raise HTTPException(status_code=500, detail="Failed to deploy new Cloudflare Worker")
    return {"status": 200, "msg": "Worker deployed successfully", "url": new_url}

@app.post("/api/workers/delete")
async def delete_worker(key: str, data: dict = Body(...)):
    verify_key(key)
    url = data.get("url")
    if not url:
        raise HTTPException(status_code=400, detail="Missing URL")

    await delete_cloudflare_worker_script(url)

    def run_delete():
        conn = get_db_connection()
        try:
            conn.execute("DELETE FROM workers WHERE url = ?", (url,))
            conn.commit()
        finally:
            conn.close()
    await db_thread(run_delete)
    return {"status": 200, "msg": "Worker deleted"}

@app.post("/api/workers/replace")
async def replace_worker(key: str, data: dict = Body(...)):
    verify_key(key)
    url = data.get("url")
    if not url:
        raise HTTPException(status_code=400, detail="Missing URL")

    new_url = await deploy_new_cloudflare_worker()
    if not new_url:
        raise HTTPException(status_code=500, detail="Failed to deploy new Cloudflare Worker")

    await delete_cloudflare_worker_script(url)

    def run_db_replace():
        conn = get_db_connection()
        try:
            conn.execute("DELETE FROM workers WHERE url = ?", (url,))
            conn.commit()
        finally:
            conn.close()
    await db_thread(run_db_replace)
    return {"status": 200, "msg": "Worker replaced successfully", "new_url": new_url}

@app.post("/github_push_event")
async def github_webhook(request: Request):
    try:
        payload = await request.json()
        ref = payload.get("ref", "")
        if "refs/heads/main" not in ref:
            return {"status": "ignored", "reason": "not main branch push"}

        log("📢 [GITHUB WEBHOOK] Push detected on main branch. Starting active workers hot sync...")
        script_code = await fetch_latest_worker_script_from_github()

        def get_all_active_workers():
            conn = get_db_connection()
            try:
                rows = conn.execute("SELECT url FROM workers WHERE status = 'healthy'").fetchall()
                return [r["url"] for r in rows]
            finally:
                conn.close()

        active_urls = await db_thread(get_all_active_workers)
        updated_count = 0
        for url in active_urls:
            name = url.replace("https://", "").replace("http://", "").split(".")[0]
            success = await update_existing_cloudflare_worker_script(name, script_code)
            if success:
                updated_count += 1

        return {"status": "success", "msg": f"Synced {updated_count} active workers with GitHub"}
    except Exception as e:
        log(f"❌ [GITHUB WEBHOOK] Sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

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

# ============================================================
# ⚠️ TELEGRAM CORE FUNCTIONS
# ============================================================
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
    asyncio.create_task(workers_health_check_loop())
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
    log("URLKING HYBRID SYSTEM ONLINE & READY (INVISIBLE MODE ACTIVE)")
