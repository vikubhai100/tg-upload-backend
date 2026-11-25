import os
import sys
from pathlib import Path

# ============================================================
# ☁️ CLOUDFLARE CONFIG
# 🔐 ENV-ONLY (v4.0): hardcoded secrets HATA diye — ye values
#    github pe leak ho chuki thi, Coolify env me NAYI (rotated) keys
#    set karo: R2_ACCESS_KEY, R2_SECRET_KEY, INTERNAL_API_KEY,
#    GOOGLE_SAFE_BROWSING_API_KEY
# ============================================================
R2_ENDPOINT = os.getenv("R2_ENDPOINT", "https://c756225d2d945ebc6c51149e7a1e3cfe.r2.cloudflarestorage.com")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "urlking")

CLOUDFLARE_TOKEN = os.getenv("CLOUDFLARE_TOKEN", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "c756225d2d945ebc6c51149e7a1e3cfe")

GOOGLE_API_KEY = os.getenv("GOOGLE_SAFE_BROWSING_API_KEY", "")
DISABLE_SAFE_BROWSING = os.getenv("DISABLE_SAFE_BROWSING_CHECK", "false").lower() == "true"


# ============================================================
# 📱 TELEGRAM CONFIG
# ============================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
SESSION_STR = os.getenv("SESSION_STRING", "")

# ============================================================
# 💾 APP CONFIG
# ============================================================
BASE_URL = os.getenv("BASE_URL", "https://storage.urlking.in")
DB_FILE_SQLITE = "/app/data/files.db"
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")
STATE_FILE = "/tmp/scan_progress.json"
LOG_FILE = "/tmp/telestore.log"

# 🔐 BOOT WARNINGS — env missing par turant dikhe (silent 403 nahi)
if not INTERNAL_API_KEY:
    print("⚠️ [config] INTERNAL_API_KEY env missing! Saare ?key= / X-Internal-Key calls 403 honge.", flush=True)
if not R2_ACCESS_KEY or not R2_SECRET_KEY:
    print("⚠️ [config] R2_ACCESS_KEY / R2_SECRET_KEY env missing! R2 uploads/downloads fail honge.", flush=True)
if not BOT_TOKEN or not API_ID or not API_HASH:
    print("⚠️ [config] BOT_TOKEN / API_ID / API_HASH env missing! Telegram client start nahi hoga.", flush=True)

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"

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

def safeFile(name):
    import re
    return re.sub(r'[<>:"/\\|?*\x00-\x1F]', '_', (name or 'file')).strip() or 'file'
