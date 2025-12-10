import os
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)

# ── Config / Env ─────────────────────────────────────────────────────────────
# New: upstream backend base URL (UAT)
BACKEND_BASE_URL = os.getenv(
    "BACKEND_BASE_URL",
    "http://10.32.34.119/check_accessory_tool"
).rstrip("/")

_frontend_origins = [
    o.strip()
    for o in os.getenv("FRONTEND_ORIGINS", "").split(",")
    if o.strip()
]
if not _frontend_origins:
    CORS(app)
else:
    CORS(app, resources={r"/api/*": {"origins": _frontend_origins}})

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[os.getenv("RATE_LIMIT_DEFAULT", "200 per hour")]
)
limiter.init_app(app)

ENFORCE_ORIGIN = os.getenv("ENFORCE_ORIGIN", "true").lower() == "true"
ALLOWED_ORIGINS = {o.lower() for o in _frontend_origins}
API_REFRESH_TOKEN = os.getenv("API_REFRESH_TOKEN", "")

# ── Rate limit configs (same env names as before) ────────────────────────────
RATE_LIMIT_META = os.getenv("RATE_LIMIT_META", "30/minute;1000/day")
RATE_LIMIT_SUGGEST = os.getenv("RATE_LIMIT_SUGGEST", "60/minute;1500/day")
RATE_LIMIT_SEARCH = os.getenv("RATE_LIMIT_SEARCH", "20/minute;500/day")
RATE_LIMIT_REFRESH = os.getenv("RATE_LIMIT_REFRESH", "5/hour;20/day")

# ── Security / Origins ───────────────────────────────────────────────────────
def _normalize_origin(value: str) -> str:
    try:
        p = urlparse(value)
        if p.scheme and p.netloc:
            return f"{p.scheme.lower()}://{p.netloc.lower()}"
    except Exception:
        pass
    return ""

def _origin_allowed() -> bool:
    if not ENFORCE_ORIGIN or not ALLOWED_ORIGINS:
        return True
    origin = request.headers.get("Origin", "")
    ref = request.headers.get("Referer", "")
    if origin:
        return _normalize_origin(origin) in ALLOWED_ORIGINS
    if ref:
        return _normalize_origin(ref) in ALLOWED_ORIGINS
    return False

@app.before_request
def _block_unknown_origins():
    if request.path == "/api/healthz":
        return
    if request.path == "/api/refresh":
        # refresh still guarded by our token
        auth = request.headers.get("Authorization", "")
        if API_REFRESH_TOKEN and auth == f"Bearer {API_REFRESH_TOKEN}":
            return
    if request.path.startswith("/api/"):
        if not _origin_allowed():
            return jsonify({"error": "origin not allowed"}), 403

@app.after_request
def _security_headers(resp):
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    resp.headers["Content-Security-Policy"] = (
        "default-src 'none'; frame-ancestors 'none'; base-uri 'none'"
    )
    resp.headers["Strict-Transport-Security"] = (
        "max-age=31536000; includeSubDomains"
    )
    return resp

# ── Upstream proxy helpers ───────────────────────────────────────────────────
def _upstream_url(path: str) -> str:
    """
    Build full upstream URL by prefixing our path with the UAT base.
    Example: /api/search -> http://10.32.34.119/check_accessory_tool/api/search
    """
    return f"{BACKEND_BASE_URL}{path}"

def _forward_headers() -> dict:
    """
    Decide which headers to forward upstream.
    Typically Authorization and maybe others if needed.
    """
    headers = {}
    # Forward Authorization if present (for /api/refresh etc.)
    if "Authorization" in request.headers:
        headers["Authorization"] = request.headers["Authorization"]
    return headers

def _proxy_upstream(method: str):
    """
    Generic proxy function: forwards the current request to the upstream backend
    and returns its response (status + body + content-type).
    """
    url = _upstream_url(request.path)
    headers = _forward_headers()

    try:
        if method.upper() == "GET":
            upstream_resp = requests.get(
                url,
                params=request.args,
                headers=headers,
                timeout=30,
            )
        else:
            json_body = request.get_json(force=False, silent=True)
            upstream_resp = requests.request(
                method.upper(),
                url,
                params=request.args,
                json=json_body,
                headers=headers,
                timeout=30,
            )
    except requests.RequestException:
        # Upstream is down/unreachable
        return jsonify({"error": "Upstream accessory tool backend unavailable"}), 502

    # Build Flask response from upstream response
    resp = make_response(upstream_resp.content, upstream_resp.status_code)
    content_type = upstream_resp.headers.get("Content-Type")
    if content_type:
        resp.headers["Content-Type"] = content_type
    return resp

# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/api/healthz")
def health():
    # Simple local health; optional: you could also ping upstream here if desired.
    return {"ok": True}

@limiter.limit(RATE_LIMIT_META)
@app.get("/api/meta")
def api_meta():
    # Proxy GET /api/meta to upstream
    return _proxy_upstream("GET")

@limiter.limit(RATE_LIMIT_SUGGEST)
@app.get("/api/suggest")
def api_suggest():
    # Proxy GET /api/suggest to upstream
    return _proxy_upstream("GET")

@limiter.limit(RATE_LIMIT_SEARCH)
@app.post("/api/search")
def api_search():
    # Proxy POST /api/search to upstream
    return _proxy_upstream("POST")

@limiter.limit(RATE_LIMIT_REFRESH)
@app.post("/api/refresh")
def api_refresh():
    # We already checked Authorization in before_request for /api/refresh.
    # If it reached here, the token was accepted (or no token is configured).
    if API_REFRESH_TOKEN:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {API_REFRESH_TOKEN}":
            return jsonify({"error": "unauthorized"}), 401

    # Proxy POST /api/refresh to upstream (if upstream supports it)
    return _proxy_upstream("POST")

@app.errorhandler(429)
def _ratelimit_handler(e):
    return jsonify({"error": "Too many requests. Please try again later."}), 429

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
