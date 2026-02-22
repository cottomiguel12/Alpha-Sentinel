import os, requests

def fetch_config():
    base = os.environ["OPENCLOW_INTERNAL_URL"].rstrip("/")
    token = os.environ["SENTINEL_PROVISIONER_TOKEN"]
    url = f"{base}/internal/alpha-sentinel/config"
    r = requests.get(url, headers={"X-Provisioner-Token": token}, timeout=10)
    r.raise_for_status()
    return r.json()

def is_placeholder_key(api_key: str) -> bool:
    if not api_key:
        return True
    return "PASTE_YOUR_UNUSUALWHALES_KEY_HERE" in api_key
