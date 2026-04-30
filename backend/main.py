import os
import uuid
import tempfile
import asyncio
import time
import sqlite3
import threading
import math
import boto3
from botocore.config import Config
from pathlib import Path
from urllib.parse import quote
from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, Response, RedirectResponse
from fastapi.staticfiles import StaticFiles
import sys
import aiohttp
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import InputFileBig

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

# Config
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
# Frontend folder ka path define karein
CURRENT_DIR = Path(__file__).parent
FRONTEND_DIR = CURRENT_DIR / "frontend"

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_file = FRONTEND_DIR / "index.html"
    if index_file.exists():
        return index_file.read_text()
    return "<h1>Frontend Error: index.html not found in /frontend folder</h1>"

# Static files (CSS/JS) serve karne ke liye agar zaroorat ho
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

# ============================================================
# 🗄️ DATABASE & MIGRATION
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
    try:
        conn.execute("ALTER TABLE files ADD COLUMN storage_type TEXT DEFAULT 'telegram'")
        conn.execute("ALTER TABLE files ADD COLUMN r2_key TEXT")
    except: pass
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    conn.close()

def get_file_entry(short_id):
    with _db_lock:
        conn = sqlite3.connect(DB_FILE_SQLITE); conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM files WHERE short_id = ?", (short_id,)).fetchone()
        conn.close()
    return dict(row) if row else None

# ============================================================
# 📂 API ROUTES (For your Dashboard)
# ============================================================

@app.get("/files")
async def list_files(key: str, page: int = 1, limit: int = 10):
    if key != INTERNAL_API_KEY: raise HTTPException(status_code=403)
    conn = sqlite3.connect(DB_FILE_SQLITE); conn.row_factory = sqlite3.Row
    offset = (page - 1) * limit
    total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    rows = conn.execute("SELECT * FROM files ORDER BY rowid DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    conn.close()
    
    return {
        "files": [{
            "short_id": r["short_id"], 
            "filename": r["filename"], 
            "size": format_size(r["size"]),
            "download_link": f"{BASE_URL}/download/{r['short_id']}"
        } for r in rows],
        "total": total, "page": page, "total_pages": math.ceil(total / limit) if total > 0 else 1
    }

# ============================================================
# ⬇️ SMART DOWNLOAD (R2 Redirect + TG Stream)
# ============================================================
@app.get("/download/{short_id}")
async def download_handle(request: Request, short_id: str):
    entry = get_file_entry(short_id)
    if not entry: raise HTTPException(status_code=404)

    # 🚀 R2 Redirect
    if entry.get("storage_type") == "r2":
        url = r2_client.generate_presigned_url('get_object', Params={
            'Bucket': R2_BUCKET_NAME, 'Key': entry["r2_key"],
            'ResponseContentDisposition': f"attachment; filename=\"{entry['filename']}\""
        }, ExpiresIn=7200)
        return RedirectResponse(url=url)

    # 🔵 Telegram Stream (Yahan aapka purana 16-pipe logic aayega)
    # ... (Streaming Response code)
    return JSONResponse({"msg": "TG Streaming logic here"})

# ============================================================
# 🗑️ SMART DELETE (Physical Cleanup)
# ============================================================
@app.get("/api/file/delete")
async def delete_handle(key: str, file_code: str):
    if key != INTERNAL_API_KEY: raise HTTPException(status_code=403)
    entry = get_file_entry(file_code)
    if entry and entry.get("storage_type") == "r2":
        try: r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=entry["r2_key"])
        except: pass
    
    conn = sqlite3.connect(DB_FILE_SQLITE)
    conn.execute("DELETE FROM files WHERE short_id = ?", (file_code,))
    conn.commit(); conn.close()
    return {"status": 200, "msg": "Deleted"}

@app.on_event("startup")
async def startup(): init_db(); log("🚀 Backend Ready with Frontend!")
