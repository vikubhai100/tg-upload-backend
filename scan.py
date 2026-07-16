import sqlite3
import requests
import time
import json
import os
import sys

# ==========================================
# 🛠️ CONFIGURATION
# ==========================================
VT_API_KEY = "5d93c3a803e809b42dd04d4a0859830db80887eb21d81fd630f33b2539e779fe"
DB_PATH = "/app/data/files.db" # Aapke FastAPI code ke hisaab se DB path
STATE_FILE = "/tmp/scan_progress.json"
DELAY_SEC = 16 # API rate limit (4 req/min for VT Free)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def update_state(state_dict):
    """Progress ko JSON file me save karega taaki FastAPI usko read kar sake"""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state_dict, f)
    except Exception as e:
        print(f"Error saving state: {e}")

def run_scan():
    print("🚀 Starting URLKING Background Virus Scanner...")

    # Init state
    state = {
        "status": "initializing",
        "total": 0,
        "scanned": 0,
        "infected": [],
        "safe": 0,
        "not_found_in_vt": 0
    }
    update_state(state)

    try:
        conn = get_db()
        # Sirf executable aur compressed files ko filter karo jisme risk hota hai
        query = """
            SELECT short_id, filename, file_hash 
            FROM files 
            WHERE (filename LIKE '%.apk' 
               OR filename LIKE '%.exe' 
               OR filename LIKE '%.zip' 
               OR filename LIKE '%.rar')
               AND file_hash IS NOT NULL
        """
        files = conn.execute(query).fetchall()
        conn.close()

        state["total"] = len(files)
        state["status"] = "running"
        update_state(state)

        print(f"📁 Found {len(files)} risky files to scan.")

        headers = {
            "accept": "application/json",
            "x-apikey": VT_API_KEY
        }

        for i, file in enumerate(files):
            short_id = file["short_id"]
            filename = file["filename"]
            file_hash = file["file_hash"]

            print(f"\n⏳ [{i + 1}/{len(files)}] Scanning Hash for: {filename} ({short_id})")

            # VirusTotal API V3 - File Report endpoint
            vt_url = f"https://www.virustotal.com/api/v3/files/{file_hash}"

            try:
                response = requests.get(vt_url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    stats = data.get("data", {}).get("attributes", {}).get("last_analysis_stats", {})

                    malicious_count = stats.get("malicious", 0) + stats.get("suspicious", 0)

                    if malicious_count >= 2:
                        print(f"🚨 VIRUS DETECTED! Flagged by {malicious_count} engines.")
                        state["infected"].append({
                            "file_code": short_id,
                            "filename": filename,
                            "engines": malicious_count
                        })
                    else:
                        print("✅ Safe")
                        state["safe"] += 1

                elif response.status_code == 404:
                    print("⚠️ Hash not found in VT database (Skipping).")
                    state["not_found_in_vt"] += 1
                else:
                    print(f"❌ API Error: {response.status_code} - {response.text}")

            except Exception as req_err:
                print(f"❌ Network Error: {req_err}")

            # Update progress state
            state["scanned"] = i + 1
            update_state(state)

            # API Rate Limit protection
            if i < len(files) - 1:
                time.sleep(DELAY_SEC)

        state["status"] = "completed"
        update_state(state)
        print("\n🎉 Scanning Completed Successfully!")

    except Exception as e:
        state["status"] = f"error: {str(e)}"
        update_state(state)
        print(f"\n💥 Scanner crashed: {e}")

if __name__ == "__main__":
    if VT_API_KEY == "YAHAN_APNI_VIRUSTOTAL_API_KEY_DAALEIN":
        print("Bhai, pehle VT_API_KEY daalo script me!")
        sys.exit(1)

    run_scan()