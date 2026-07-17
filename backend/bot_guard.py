import os
from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse

INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "super_secret_key_123")

async def bot_guard_middleware(request: Request, call_next):
    ua = request.headers.get("user-agent", "").lower()

    # ✅ Allow GitHub webhooks
    if "github-hookshot" in ua:
        return await call_next(request)

    # ✅ Allow internal server-to-server calls using secret key header
    internal_key = request.headers.get("X-Internal-Key", "")
    if internal_key == INTERNAL_API_KEY:
        return await call_next(request)

    # ✅ Allow all /api/ routes (public API endpoints)
    if request.url.path.startswith("/api/"):
        return await call_next(request)

    sec_ch_ua = request.headers.get("sec-ch-ua", "").lower()

    # 🚫 Block known search engine crawlers on non-API routes
    if any(bot in ua for bot in ["googlebot", "google", "safebrowsing", "mediapartners", "adsbot", "bingbot", "yandex", "slurp"]):
        return HTMLResponse(content="<h1>404 Not Found</h1>", status_code=404)

    # 🚫 Block non-browser automation tools (node-fetch removed — it's our own server)
    if any(b in ua for b in ["python", "curl", "wget", "httpie", "postman", "crawler", "spider", "telegram", "axios", "libwww"]):
        return JSONResponse(status_code=403, content={"error": "Bot Access Denied"})

    # 🚫 Block headless browsers
    if "headless" in sec_ch_ua or any(h in ua for h in ["headlesschrome", "puppeteer", "playwright", "selenium"]):
        return JSONResponse(status_code=403, content={"error": "Headless Engine Detected"})

    return await call_next(request)
