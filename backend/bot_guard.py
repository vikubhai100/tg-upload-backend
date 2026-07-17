from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse

async def bot_guard_middleware(request: Request, call_next):
    ua = request.headers.get("user-agent", "").lower()
    if "github-hookshot" in ua:
        return await call_next(request)

    if request.url.path.startswith("/api/"):
        return await call_next(request)

    ua = request.headers.get("user-agent", "").lower()
    sec_ch_ua = request.headers.get("sec-ch-ua", "").lower()

    if any(bot in ua for bot in ["googlebot", "google", "safebrowsing", "mediapartners", "adsbot", "bingbot", "yandex", "slurp"]):
        return HTMLResponse(content="<h1>404 Not Found</h1>", status_code=404)

    if any(b in ua for b in ["python", "curl", "wget", "httpie", "postman", "crawler", "spider", "telegram", "axios", "node-fetch", "libwww"]):
        return JSONResponse(status_code=403, content={"error": "Bot Access Denied"})

    if "headless" in sec_ch_ua or any(h in ua for h in ["headlesschrome", "puppeteer", "playwright", "selenium"]):
        return JSONResponse(status_code=403, content={"error": "Headless Engine Detected"})

    return await call_next(request)
