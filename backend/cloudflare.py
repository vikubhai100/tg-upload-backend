import os
import json
import random
import string
import asyncio
import aiohttp
from backend.config import CLOUDFLARE_TOKEN, CLOUDFLARE_ACCOUNT_ID, log
from backend.database import get_db_connection

KV_NAMESPACE_ID = os.getenv("CLOUDFLARE_KV_NAMESPACE_ID", "")

UNIFIED_WORKER_JS = """const SECURE_SECRET = "URLKING_ANTI_SHARE_SECRET_2110";

async function verifySignature(token, signature) {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(SECURE_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"]
  );

  const sigBuffer = new Uint8Array(
    signature.match(/.{1,2}/g).map(byte => parseInt(byte, 16))
  );

  return await crypto.subtle.verify(
    "HMAC",
    key,
    sigBuffer,
    encoder.encode(token)
  );
}

class S3Client {
  constructor(accountId, accessKeyId, secretAccessKey) {
    this.accountId = accountId;
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
  }

  async getObject(bucketName, key) {
    const url = `https://${this.accountId}.r2.cloudflarestorage.com/${bucketName}/${key}`;
    const method = 'GET';
    const host = `${this.accountId}.r2.cloudflarestorage.com`;
    const datetime = new Date().toISOString().replace(/[:\-]|\.\d{3}/g, '');
    const date = datetime.substr(0, 8);

    const credentialScope = `${date}/us-east-1/s3/aws4_request`;
    const canonicalHeaders = `host:${host}\\nx-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\\nx-amz-date:${datetime}\\n`;
    const signedHeaders = 'host;x-amz-content-sha256;x-amz-date';

    const canonicalRequest = `${method}\\n/${bucketName}/${key}\\n\\n${canonicalHeaders}\\n${signedHeaders}\\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`;
    const canonicalRequestHash = await this.sha256(canonicalRequest);

    const stringToSign = `AWS4-HMAC-SHA256\\n${datetime}\\n${credentialScope}\\n${canonicalRequestHash}`;

    const signingKey = await this.getSigningKey(this.secretAccessKey, date, "us-east-1", "s3");
    const signature = await this.hmac(signingKey, stringToSign);

    const authHeader = `AWS4-HMAC-SHA256 Credential=${this.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

    return fetch(url, {
      method: method,
      headers: {
        'Authorization': authHeader,
        'x-amz-date': datetime,
        'x-amz-content-sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
      }
    });
  }

  async sha256(message) {
    const msgBuffer = new TextEncoder().encode(message);
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
    return Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');
  }

  async hmac(key, data) {
    const cryptoKey = await crypto.subtle.importKey(
      "raw", key, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
    );
    const signature = await crypto.subtle.sign("HMAC", cryptoKey, new TextEncoder().encode(data));
    return Array.from(new Uint8Array(signature)).map(b => b.toString(16).padStart(2, '0')).join('');
  }

  async getSigningKey(secret, date, region, service) {
    const kDate = await crypto.subtle.sign("HMAC", await crypto.subtle.importKey("raw", new TextEncoder().encode("AWS4" + secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]), new TextEncoder().encode(date));
    const kRegion = await crypto.subtle.sign("HMAC", await crypto.subtle.importKey("raw", kDate, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]), new TextEncoder().encode(region));
    const kService = await crypto.subtle.sign("HMAC", await crypto.subtle.importKey("raw", kRegion, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]), new TextEncoder().encode(service));
    return await crypto.subtle.sign("HMAC", await crypto.subtle.importKey("raw", kService, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]), new TextEncoder().encode("aws4_request"));
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/health") {
      return new Response("OK", { status: 200 });
    }

    const userAgent = (request.headers.get("user-agent") || "").toLowerCase();
    const bannedCrawlers = [
      'googlebot', 'mediapartners-google', 'adsbot-google', 'bingbot', 'yandexbot', 
      'baiduspider', 'twitterbot', 'facebookexternalhit', 'google-publisher-plugin',
      'lighthouse', 'chrome-lighthouse', 'duckduckbot', 'slurp', 'ia_archiver',
      'safebrowsing'
    ];
    if (bannedCrawlers.some(bot => userAgent.includes(bot))) {
      return new Response("Not Found", { status: 404 });
    }

    if (path !== "/d") {
      return new Response(`
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Nothing Here</title>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                    background-color: #ffffff;
                    color: #000000;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    justify-content: center;
                    height: 100vh;
                    margin: 0;
                    text-align: center;
                }
                .container { max-width: 600px; padding: 20px; }
                h1 { font-size: 32px; font-weight: 600; margin: 0 0 10px 0; }
                p { font-size: 16px; color: #333333; margin: 0 0 5px 0; }
                .footer { position: absolute; bottom: 20px; right: 30px; font-size: 12px; color: #666666; }
            </style>
        </head>
        <body>
            <div class="container">
                <svg width="280" height="280" viewBox="0 0 280 280" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <circle cx="140" cy="140" r="80" fill="#E0F2FE"/>
                    <path d="M110 115H170V155H110V115Z" fill="#0284C7"/>
                    <rect x="115" y="120" width="8" height="8" rx="4" fill="#F59E0B"/>
                    <rect x="127" y="120" width="8" height="8" rx="4" fill="#F59E0B"/>
                    <rect x="139" y="120" width="8" height="8" rx="4" fill="#F59E0B"/>
                    <path d="M125 145C125 140 155 140 155 145" stroke="white" stroke-width="3" stroke-linecap="round"/>
                    <circle cx="140" cy="140" r="120" stroke="#E2E8F0" stroke-width="1" stroke-dasharray="4 4"/>
                </svg>
                <h1>There is nothing here yet</h1>
                <p>If you expect something to be here, it may take some time.</p>
                <p>Please check back again later.</p>
            </div>
            <div class="footer">Powered by Cloudflare</div>
        </body>
        </html>
      `, { status: 200, headers: { "Content-Type": "text/html" } });
    }

    const token = url.searchParams.get("id");
    const signature = url.searchParams.get("sig");
    if (!token || !signature) {
      return new Response("Not Found", { status: 404 });
    }

    const isValid = await verifySignature(token, signature);
    if (!isValid) {
      return new Response("Not Found", { status: 404 });
    }

    let payload;
    try {
      const decodedStr = atob(token.replace(/-/g, "+").replace(/_/g, "/"));
      payload = JSON.parse(decodedStr);
    } catch (e) {
      return new Response("Not Found", { status: 404 });
    }

    const { k: fileKey, n: filename, e: exp, i: authorizedIp } = payload;
    const currentTime = Math.floor(Date.now() / 1000);
    if (currentTime > parseInt(exp)) {
      return new Response("Link Expired", { status: 404 });
    }

    const clientIp = request.headers.get("cf-connecting-ip") || request.headers.get("x-real-ip") || "unknown";
    function getSubnet(ip) {
      if (ip.includes(".")) {
        return ip.split(".").slice(0, 3).join(".");
      } else if (ip.includes(":")) {
        return ip.split(":").slice(0, 3).join(":");
      }
      return ip;
    }
    if (getSubnet(clientIp) !== getSubnet(authorizedIp)) {
      return new Response("Not Found", { status: 404 });
    }

    try {
      const headers = new Headers();
      headers.set("Content-Security-Policy", "default-src 'none'");
      headers.set("X-Content-Type-Options", "nosniff");
      headers.set("X-Robots-Tag", "noindex, nofollow, noarchive");
      headers.set("Content-Disposition", `attachment; filename="${filename}"`);

      if (env.dataURLKING) {
        const object = await env.dataURLKING.get(fileKey);
        if (object === null) {
          return new Response("File Not Found.", { status: 404 });
        }
        object.writeHttpMetadata(headers);
        headers.set("etag", object.httpEtag);
        return new Response(object.body, { headers });
      } 
      
      if (env.R2_ACCESS_KEY_ID && env.R2_SECRET_ACCESS_KEY && env.R2_ACCOUNT_ID) {
        const s3 = new S3Client(env.R2_ACCOUNT_ID, env.R2_ACCESS_KEY_ID, env.R2_SECRET_ACCESS_KEY);
        const bucket = env.R2_BUCKET_NAME || "urlking";
        const response = await s3.getObject(bucket, fileKey);

        if (response.status === 404) {
          return new Response("File Not Found.", { status: 404 });
        }
        if (!response.ok) {
          return new Response("Error retrieving file from storage.", { status: response.status });
        }

        for (const [key, value] of response.headers.entries()) {
          if (['content-type', 'content-length', 'etag', 'last-modified'].includes(key.toLowerCase())) {
            headers.set(key, value);
          }
        }
        return new Response(response.body, { headers });
      }

      return new Response("Storage configuration not found.", { status: 500 });

    } catch (err) {
      return new Response("Error retrieving file.", { status: 500 });
    }
  }
};
"""

# ============================================================
# 🩺 ACCURATE HEALTH CHECK
# ============================================================
async def check_worker_health(url):
    """
    Directly pings the worker's /health endpoint.
    Returns 'healthy' if HTTP status is 200, otherwise 'unhealthy'.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{url}/health", timeout=5, headers={"User-Agent": "URLKing-HealthCheck/1.0"}) as resp:
                if resp.status == 200:
                    return "healthy"
                else:
                    log(f"⚠️ [HEALTH CHECK] {url}/health returned status: {resp.status}")
                    return "unhealthy"
    except Exception as e:
        log(f"⚠️ [HEALTH CHECK] Failed to ping {url}/health: {str(e)}")
        return "unhealthy"

async def fetch_latest_worker_script_from_github():
    raw_url = "https://raw.githubusercontent.com/vikubhai100/downloadURLKING_Cloudflare_workers/main/worker.js"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(raw_url, timeout=5) as resp:
                if resp.status == 200:
                    code = await resp.text()
                    if "export default" in code or "addEventListener" in code:
                        return code
    except Exception as e:
        log(f"⚠️ [GITHUB FETCH] Failed to get latest script from GitHub: {str(e)}")
    return UNIFIED_WORKER_JS

async def fetch_all_cloudflare_workers():
    """Cloudflare API se active scripts ki list fetch karta hai."""
    api_url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/workers/scripts"
    headers = {"Authorization": f"Bearer {CLOUDFLARE_TOKEN}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, headers=headers, timeout=10) as resp:
                result = await resp.json()
                if result.get("success"):
                    return [item["id"] for item in result.get("result", [])]
    except Exception as e:
        log(f"⚠️ [CF SYNC] Cloudflare API list fetch failed: {str(e)}")
    return None

async def deploy_new_cloudflare_worker():
    random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    script_name = f"download-{random_suffix}"
    worker_url = f"https://{script_name}.urlking.workers.dev"

    log(f"⚡ [CLOUDFLARE API] Deploying fresh worker: {worker_url}")
    url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/workers/scripts/{script_name}"

    script_code = await fetch_latest_worker_script_from_github()

    metadata = {
        "main_module": "worker.js",
        "bindings": [
            {
                "name": "dataURLKING",
                "type": "r2_bucket",
                "bucket_name": "urlking"
            }
        ]
    }

    data = aiohttp.FormData()
    data.add_field('metadata', json.dumps(metadata), content_type='application/json')
    data.add_field('script', script_code, filename='worker.js', content_type='application/javascript+module')

    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_TOKEN}"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(url, data=data, headers=headers) as resp:
                result = await resp.json()
                if result.get("success"):
                    subdomain_url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/workers/scripts/{script_name}/subdomain"
                    async with session.post(subdomain_url, json={"enabled": True}, headers=headers) as s_resp:
                        s_result = await s_resp.json()
                        if s_result.get("success"):
                            log(f"✅ [CLOUDFLARE API] Subdomain enabled successfully for {script_name}")

                            is_safe = False
                            for attempt in range(5):
                                await asyncio.sleep(5)
                                if await check_worker_health(worker_url) == "healthy":
                                    is_safe = True
                                    break
                                log(f"⏳ [CLOUDFLARE API] Retrying health check for {worker_url} (attempt {attempt+1}/5)...")

                            if is_safe:
                                import time
                                conn = get_db_connection()
                                conn.execute(
                                    "INSERT OR REPLACE INTO workers (url, status, created_at) VALUES (?, 'healthy', ?)",
                                    (worker_url, int(time.time()))
                                )
                                conn.commit()
                                conn.close()
                                return worker_url
                            else:
                                log(f"❌ [CLOUDFLARE API] Newly created worker {worker_url} failed safe health check.")
                log(f"❌ [CLOUDFLARE API] Failed to deploy worker: {result}")
    except Exception as e:
        log(f"❌ [CLOUDFLARE API] Exception during deployment: {str(e)}")
    return None

async def get_active_worker():
    conn = get_db_connection()
    rows = conn.execute("SELECT url FROM workers WHERE status = 'healthy'").fetchall()
    conn.close()

    workers = [r["url"] for r in rows]
    random.shuffle(workers)

    for w in workers:
        h_status = await check_worker_health(w)
        if h_status == "healthy":
            return w
        else:
            log(f"⚠️ [ROTATOR] Worker offline/slow: {w}")

    new_worker = await deploy_new_cloudflare_worker()
    if new_worker:
        return new_worker

    return "https://download.urlking.workers.dev"

async def delete_cloudflare_worker_script(url_or_name):
    name = url_or_name.replace("https://", "").replace("http://", "").split(".")[0]

    log(f"🗑️ [CLOUDFLARE API] Deleting worker script from Cloudflare: {name}")
    api_url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/workers/scripts/{name}"
    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_TOKEN}"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.delete(api_url, headers=headers) as resp:
                result = await resp.json()
                if result.get("success"):
                    log(f"✅ [CLOUDFLARE API] Successfully deleted {name} from Cloudflare")
                    return True
                else:
                    log(f"❌ [CLOUDFLARE API] Failed to delete script {name}: {result}")
    except Exception as e:
        log(f"❌ [CLOUDFLARE API] Exception during deletion: {str(e)}")
    return False

async def update_existing_cloudflare_worker_script(name, script_code):
    log(f"🔄 [CLOUDFLARE API] Syncing latest GitHub code to active worker: {name}")
    url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/workers/scripts/{name}"

    metadata = {
        "main_module": "worker.js",
        "bindings": [
            {
                "name": "dataURLKING",
                "type": "r2_bucket",
                "bucket_name": "urlking"
            }
        ]
    }

    data = aiohttp.FormData()
    data.add_field('metadata', json.dumps(metadata), content_type='application/json')
    data.add_field('script', script_code, filename='worker.js', content_type='application/javascript+module')

    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_TOKEN}"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(url, data=data, headers=headers) as resp:
                result = await resp.json()
                if result.get("success"):
                    log(f"✅ [CLOUDFLARE API] Successfully updated code in {name}")
                    return True
                else:
                    log(f"❌ [CLOUDFLARE API] Failed to update script code {name}: {result}")
    except Exception as e:
        log(f"❌ [CLOUDFLARE API] Exception during update: {str(e)}")
    return False

async def workers_health_check_loop():
    """
    1. Cloudflare se deleted workers ko DB se SAF kar dega.
    2. Only 'd1' ya 'download' se start hone wale active workers ko hi rakhega.
    """
    await asyncio.sleep(10)
    while True:
        try:
            # Step A: Cloudflare se live workers fetch karo
            cf_scripts = await fetch_all_cloudflare_workers()

            conn = get_db_connection()
            rows = conn.execute("SELECT url, status, created_at FROM workers").fetchall()
            conn.close()

            if cf_scripts is not None:
                for r in rows:
                    url = r["url"]
                    script_name = url.replace("https://", "").replace("http://", "").split(".")[0]

                    # Skip deletion check for Account B worker
                    if "urlkingworker" in script_name:
                        continue

                    # Filter ghost workers or non-matching workers
                    is_valid_prefix = script_name.startswith("d1") or script_name.startswith("download")

                    if script_name not in cf_scripts or not is_valid_prefix:
                        log(f"🧹 [AUTO FILTER] Removing ghost/deleted worker from DB: {url}")
                        conn = get_db_connection()
                        conn.execute("DELETE FROM workers WHERE url = ?", (url,))
                        conn.commit()
                        conn.close()
                        continue

            # Step B: Baki workers ki Health Ping Check karo
            conn = get_db_connection()
            rows = conn.execute("SELECT url, status, created_at FROM workers").fetchall()
            conn.close()

            for r in rows:
                url = r["url"]
                status = r["status"]
                created_at = r["created_at"] if r["created_at"] else 0

                import time
                age_seconds = int(time.time()) - created_at
                if age_seconds < 120:
                    continue

                h_status = await check_worker_health(url)

                if h_status == "unhealthy":
                    conn = get_db_connection()
                    conn.execute("UPDATE workers SET status = 'unhealthy' WHERE url = ?", (url,))
                    conn.commit()
                    conn.close()
                    log(f"⚠️ [HEALTH CHECK] Worker {url} is offline/slow, marked unhealthy")
                else:
                    if status != "healthy":
                        conn = get_db_connection()
                        conn.execute("UPDATE workers SET status = 'healthy' WHERE url = ?", (url,))
                        conn.commit()
                        conn.close()

        except Exception as e:
            log(f"❌ [HEALTH CHECK] Loop exception: {str(e)}")
        await asyncio.sleep(60)
