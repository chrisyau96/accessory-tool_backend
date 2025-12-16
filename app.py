import os
import re
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)

# ── Config / Env ─────────────────────────────────────────────────────────────
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

# ── Rate limit configs ───────────────────────────────────────────────────────
RATE_LIMIT_META = os.getenv("RATE_LIMIT_META", "30/minute;1000/day")
RATE_LIMIT_SUGGEST = os.getenv("RATE_LIMIT_SUGGEST", "60/minute;1500/day")
RATE_LIMIT_SEARCH = os.getenv("RATE_LIMIT_SEARCH", "20/minute;500/day")
RATE_LIMIT_REFRESH = os.getenv("RATE_LIMIT_REFRESH", "5/hour;20/day")

# ── Suggest broad-match tuning ───────────────────────────────────────────────
SUGGEST_BROAD_MATCH = os.getenv("SUGGEST_BROAD_MATCH", "true").lower() == "true"
SUGGEST_FALLBACK_MIN_RESULTS = int(os.getenv("SUGGEST_FALLBACK_MIN_RESULTS", "1"))
SUGGEST_FALLBACK_TIMEOUT_SEC = int(os.getenv("SUGGEST_FALLBACK_TIMEOUT_SEC", "30"))

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
    return f"{BACKEND_BASE_URL}{path}"

def _forward_headers() -> dict:
    headers = {}
    if "Authorization" in request.headers:
        headers["Authorization"] = request.headers["Authorization"]
    return headers

def _proxy_upstream(method: str):
    url = _upstream_url(request.path)
    headers = _forward_headers()

    try:
        if method.upper() == "GET":
            upstream_resp = requests.get(
                url,
                params=request.args,
                headers=headers,
                timeout=SUGGEST_FALLBACK_TIMEOUT_SEC,
            )
        else:
            json_body = request.get_json(force=False, silent=True)
            upstream_resp = requests.request(
                method.upper(),
                url,
                params=request.args,
                json=json_body,
                headers=headers,
                timeout=SUGGEST_FALLBACK_TIMEOUT_SEC,
            )
    except requests.RequestException:
        return jsonify({"error": "Upstream accessory tool backend unavailable"}), 502

    resp = make_response(upstream_resp.content, upstream_resp.status_code)
    content_type = upstream_resp.headers.get("Content-Type")
    if content_type:
        resp.headers["Content-Type"] = content_type
    return resp

# ── Broad-match helpers (used only for /api/suggest) ─────────────────────────
def _extract_list_container(payload):
    """
    Returns (items_list, wrap_fn) where wrap_fn(new_items) returns same payload shape.
    Supports:
      - list payload
      - dict payload with items under common keys
    """
    if isinstance(payload, list):
        return payload, (lambda new_items: new_items)

    if isinstance(payload, dict):
        for k in ("suggestions", "results", "items", "data"):
            v = payload.get(k)
            if isinstance(v, list):
                def _wrap(new_items, _k=k, _p=payload):
                    out = dict(_p)
                    out[_k] = new_items
                    return out
                return v, _wrap

    return None, None

def _tokenize_query(q: str):
    q = (q or "").strip().lower()
    # Split on whitespace and punctuation-ish, keep alnum chunks
    tokens = re.findall(r"[a-z0-9]+", q)
    # de-dup while keeping order
    seen = set()
    out = []
    for t in tokens:
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out

def _item_text(it) -> str:
    if not isinstance(it, dict):
        return str(it).lower()

    # Try common fields (your upstream may differ; this is defensive)
    parts = []
    for key in (
        "name", "itemName", "item_name", "productName", "product_name",
        "title", "displayName", "display_name",
        "brand", "brandName", "brand_name",
        "model", "modelNo", "model_no",
        "code", "sku", "id"
    ):
        val = it.get(key)
        if isinstance(val, str) and val.strip():
            parts.append(val.strip().lower())

    # Fallback: include any string values (still safe but broader)
    if not parts:
        for v in it.values():
            if isinstance(v, str) and v.strip():
                parts.append(v.strip().lower())

    return " ".join(parts)

def _token_match(haystack: str, tok: str) -> bool:
    if not tok:
        return False
    if len(tok) <= 2:
        # avoid crazy over-match for very short tokens like "ro"
        return re.search(rf"\b{re.escape(tok)}\b", haystack) is not None
    return tok in haystack

def _pick_primary_token(tokens):
    # Pick the "strongest" token to query upstream for candidates
    # (longest token usually gives fewer but more relevant candidates)
    if not tokens:
        return ""
    return sorted(tokens, key=lambda x: len(x), reverse=True)[0]

def _item_key(it):
    if isinstance(it, dict):
        for k in ("sku", "code", "id", "value"):
            v = it.get(k)
            if isinstance(v, (str, int)) and str(v).strip():
                return f"{k}:{str(v).strip()}"
    return f"repr:{repr(it)}"

def _score_item(it_text: str, tokens, raw_q: str) -> int:
    score = 0
    # bonus if raw query (condensed spaces) appears as substring
    condensed = " ".join(tokens)
    if condensed and condensed in it_text:
        score += 20
    # token-wise bonuses
    for t in tokens:
        if _token_match(it_text, t):
            score += 10
            if it_text.startswith(t):
                score += 3
    return score

def _broad_suggest_response():
    """
    Broad-match enhancement:
    - If q has multiple tokens, call upstream with full q first.
    - If results are empty / too few, call upstream with primary token to get candidates,
      then filter candidates by requiring ALL tokens to match somewhere in the item text.
    - De-dup and rank.
    - Preserve upstream response shape.
    """
    q = request.args.get("q") or request.args.get("query") or request.args.get("term") or ""
    tokens = _tokenize_query(q)

    # If not multi-token or disabled, just proxy
    if not SUGGEST_BROAD_MATCH or len(tokens) <= 1:
        return _proxy_upstream("GET")

    headers = _forward_headers()
    url = _upstream_url(request.path)

    # 1) Try upstream with original query first
    try:
        upstream_full = requests.get(url, params=request.args, headers=headers, timeout=SUGGEST_FALLBACK_TIMEOUT_SEC)
    except requests.RequestException:
        return jsonify({"error": "Upstream accessory tool backend unavailable"}), 502

    # If upstream didn't return JSON or failed, just return it as-is
    try:
        payload_full = upstream_full.json()
    except Exception:
        resp = make_response(upstream_full.content, upstream_full.status_code)
        ct = upstream_full.headers.get("Content-Type")
        if ct:
            resp.headers["Content-Type"] = ct
        return resp

    items_full, wrap_full = _extract_list_container(payload_full)
    if items_full is None:
        # Unknown shape -> return upstream untouched
        resp = make_response(upstream_full.content, upstream_full.status_code)
        ct = upstream_full.headers.get("Content-Type")
        if ct:
            resp.headers["Content-Type"] = ct
        return resp

    # If enough results already, keep upstream behavior (but we could still re-rank if you want)
    if isinstance(items_full, list) and len(items_full) >= SUGGEST_FALLBACK_MIN_RESULTS:
        return jsonify(payload_full), upstream_full.status_code

    # 2) Fallback: query upstream by primary token to get a broader candidate pool
    primary = _pick_primary_token(tokens)
    if not primary:
        return jsonify(payload_full), upstream_full.status_code

    args2 = request.args.to_dict(flat=True)
    args2["q"] = primary  # ensure upstream gets something it can match

    try:
        upstream_primary = requests.get(url, params=args2, headers=headers, timeout=SUGGEST_FALLBACK_TIMEOUT_SEC)
    except requests.RequestException:
        return jsonify(payload_full), upstream_full.status_code  # best effort: return the original

    try:
        payload_primary = upstream_primary.json()
    except Exception:
        # Can't parse fallback; return original
        return jsonify(payload_full), upstream_full.status_code

    items_primary, wrap_primary = _extract_list_container(payload_primary)
    if not isinstance(items_primary, list):
        return jsonify(payload_full), upstream_full.status_code

    # 3) Filter candidates by requiring ALL tokens to be present
    filtered = []
    for it in items_primary:
        text = _item_text(it)
        if all(_token_match(text, t) for t in tokens):
            filtered.append(it)

    # 4) Merge with full results (if any), de-dup, rank
    merged = []
    seen = set()

    for it in (items_full or []):
        k = _item_key(it)
        if k not in seen:
            seen.add(k)
            merged.append(it)

    for it in filtered:
        k = _item_key(it)
        if k not in seen:
            seen.add(k)
            merged.append(it)

    # Rank by our broad-match score
    merged.sort(key=lambda it: _score_item(_item_text(it), tokens, q), reverse=True)

    # Preserve the "full query" payload shape (preferred), else fallback payload shape
    out_payload = wrap_full(merged) if wrap_full else (wrap_primary(merged) if wrap_primary else merged)
    return jsonify(out_payload), upstream_full.status_code

# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/api/healthz")
def health():
    return {"ok": True}

@limiter.limit(RATE_LIMIT_META)
@app.get("/api/meta")
def api_meta():
    return _proxy_upstream("GET")

@limiter.limit(RATE_LIMIT_SUGGEST)
@app.get("/api/suggest")
def api_suggest():
    # Broad-match enhanced suggest (multi-token query)
    return _broad_suggest_response()

@limiter.limit(RATE_LIMIT_SEARCH)
@app.post("/api/search")
def api_search():
    return _proxy_upstream("POST")

@limiter.limit(RATE_LIMIT_REFRESH)
@app.post("/api/refresh")
def api_refresh():
    if API_REFRESH_TOKEN:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {API_REFRESH_TOKEN}":
            return jsonify({"error": "unauthorized"}), 401
    return _proxy_upstream("POST")

@app.errorhandler(429)
def _ratelimit_handler(e):
    return jsonify({"error": "Too many requests. Please try again later."}), 429

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
