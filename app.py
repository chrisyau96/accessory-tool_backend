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

# ── Broad-match tuning ───────────────────────────────────────────────────────
# Applies local broad-match filtering + ranking on top of upstream responses.
SUGGEST_BROAD_MATCH = os.getenv("SUGGEST_BROAD_MATCH", "true").lower() == "true"
SEARCH_BROAD_MATCH = os.getenv("SEARCH_BROAD_MATCH", str(SUGGEST_BROAD_MATCH)).lower() == "true"

# If upstream returns fewer than this, we will fan out to additional upstream queries (variants)
BROAD_MATCH_FALLBACK_MIN_RESULTS = int(os.getenv("BROAD_MATCH_FALLBACK_MIN_RESULTS", "1"))

# Total upstream calls per request: 1 (original) + (max_calls-1) variants
BROAD_MATCH_MAX_UPSTREAM_CALLS = int(os.getenv("BROAD_MATCH_MAX_UPSTREAM_CALLS", "4"))

# Cap merged candidates after de-dupe (helps keep response small / predictable)
BROAD_MATCH_MAX_MERGED_RESULTS = int(os.getenv("BROAD_MATCH_MAX_MERGED_RESULTS", "50"))

# Always re-rank results for suggest so "more parts matched => higher in list"
SUGGEST_RERANK_ALWAYS = os.getenv("SUGGEST_RERANK_ALWAYS", "true").lower() == "true"
SEARCH_RERANK_ALWAYS = os.getenv("SEARCH_RERANK_ALWAYS", "false").lower() == "true"

BROAD_MATCH_TIMEOUT_SEC = int(os.getenv("BROAD_MATCH_TIMEOUT_SEC", "30"))

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
                timeout=BROAD_MATCH_TIMEOUT_SEC,
            )
        else:
            json_body = request.get_json(force=False, silent=True)
            upstream_resp = requests.request(
                method.upper(),
                url,
                params=request.args,
                json=json_body,
                headers=headers,
                timeout=BROAD_MATCH_TIMEOUT_SEC,
            )
    except requests.RequestException:
        return jsonify({"error": "Upstream accessory tool backend unavailable"}), 502

    resp = make_response(upstream_resp.content, upstream_resp.status_code)
    content_type = upstream_resp.headers.get("Content-Type")
    if content_type:
        resp.headers["Content-Type"] = content_type
    return resp

# ── Broad-match helpers ──────────────────────────────────────────────────────
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

    # Fallback: include any string values
    if not parts:
        for v in it.values():
            if isinstance(v, str) and v.strip():
                parts.append(v.strip().lower())

    return " ".join(parts)

def _token_match(haystack: str, tok: str) -> bool:
    if not tok:
        return False
    # Keep very-short tokens from overmatching too aggressively
    if len(tok) <= 2:
        return re.search(rf"\b{re.escape(tok)}\b", haystack) is not None
    return tok in haystack

def _word_boundary_at(text: str, pos: int) -> bool:
    return pos == 0 or (pos > 0 and not text[pos - 1].isalnum())

def _word_boundary_after(text: str, end: int) -> bool:
    return end >= len(text) or (end < len(text) and not text[end].isalnum())

def _match_stats(text: str, tokens: list[str], raw_q: str):
    """
    Returns:
      matched_count: int
      quality_score: int (higher is better)
    Primary sort is matched_count (more parts matched => higher rank).
    """
    if not tokens:
        return 0, 0

    matched_positions = []
    quality = 0
    matched_count = 0

    for t in tokens:
        pos = text.find(t)
        if pos == -1:
            continue
        matched_count += 1
        end = pos + len(t)

        # Base points for matching this token; longer tokens are more valuable
        quality += 20 + min(len(t), 10)

        # Better if token aligns to word boundaries / starts
        if _word_boundary_at(text, pos):
            quality += 10
        if _word_boundary_at(text, pos) and _word_boundary_after(text, end):
            quality += 6  # full-word match

        # Earlier matches slightly better
        if pos == 0:
            quality += 6
        elif pos < 15:
            quality += 3
        elif pos < 40:
            quality += 1

        matched_positions.append(pos)

    # Phrase/sequence bonus (helps "hx9991 toothbrush" rank above just "hx9991")
    condensed = " ".join(_tokenize_query(raw_q))
    if condensed and condensed in text:
        quality += 35

    # Tokens appearing in order
    if len(matched_positions) >= 2 and matched_positions == sorted(matched_positions):
        quality += 10

    return matched_count, quality

def _item_key(it):
    if isinstance(it, dict):
        for k in ("sku", "code", "id", "value"):
            v = it.get(k)
            if isinstance(v, (str, int)) and str(v).strip():
                return f"{k}:{str(v).strip()}"
    return f"repr:{repr(it)}"

def _dedup_keep_order(items: list):
    out = []
    seen = set()
    for it in items:
        k = _item_key(it)
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out

def _guess_query_key_from_args():
    # prefer whatever the caller actually used
    for k in ("q", "query", "term"):
        if request.args.get(k):
            return k
    return "q"

def _guess_query_key_from_body(body: dict):
    for k in ("q", "query", "term"):
        v = body.get(k)
        if isinstance(v, str):
            return k
    return "q"

def _variants_from_token(t: str):
    """
    Generate broader query variants to get a larger candidate pool from upstream.
    Example: 'hx9991' -> ['hx9991', 'hx', 'hx999', 'hx99', 'hx9', '9991']
             'phili'  -> ['phili', 'phil', 'phi']
    """
    t = (t or "").strip().lower()
    if not t:
        return []

    variants = [t]

    letters = "".join(re.findall(r"[a-z]+", t))
    digits = "".join(re.findall(r"[0-9]+", t))

    # Useful for model numbers like hx9991 -> hx
    if letters and letters != t:
        variants.append(letters)

    if digits and digits != t:
        variants.append(digits)

    # Prefixes (broader). Keep >= 3 usually, but allow 2 for things like 'hx'.
    for n in (6, 5, 4, 3, 2):
        if len(t) > n:
            variants.append(t[:n])

    if letters:
        for n in (4, 3, 2):
            if len(letters) > n:
                variants.append(letters[:n])

    # de-dup preserve order
    out = []
    seen = set()
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out

def _build_upstream_query_variants(raw_q: str):
    """
    Build a short list of upstream query strings to try (in order),
    excluding the original raw_q (which is always tried first).
    """
    tokens = _tokenize_query(raw_q)
    if not tokens:
        return []

    # Try longer tokens first (they usually narrow the candidate pool in a useful way)
    tokens_sorted = sorted(tokens, key=len, reverse=True)

    variants = []
    for t in tokens_sorted:
        variants.extend(_variants_from_token(t))

    # de-dup, remove exact original normalized duplicates
    raw_norm = (raw_q or "").strip().lower()
    out = []
    seen = set()
    for v in variants:
        if not v:
            continue
        if v == raw_norm:
            continue
        if v in seen:
            continue
        seen.add(v)
        out.append(v)

    return out

def _call_upstream_get(url: str, headers: dict, args: dict):
    try:
        return requests.get(url, params=args, headers=headers, timeout=BROAD_MATCH_TIMEOUT_SEC)
    except requests.RequestException:
        return None

def _call_upstream_post(url: str, headers: dict, args: dict, body: dict):
    try:
        return requests.post(url, params=args, json=body, headers=headers, timeout=BROAD_MATCH_TIMEOUT_SEC)
    except requests.RequestException:
        return None

def _broad_match_response_for_get(always_rerank: bool):
    """
    Broad match for GET endpoints (/api/suggest):
      - Try upstream with original query
      - If too few, fan out to variant upstream queries to gather candidates
      - Locally rank so: more tokens matched => higher rank
    """
    q_key = _guess_query_key_from_args()
    raw_q = request.args.get(q_key, "") or ""
    tokens = _tokenize_query(raw_q)

    # If no query, just proxy
    if not raw_q.strip():
        return _proxy_upstream("GET")

    url = _upstream_url(request.path)
    headers = _forward_headers()

    # 1) Original upstream call
    upstream_full = _call_upstream_get(url, headers, request.args)
    if upstream_full is None:
        return jsonify({"error": "Upstream accessory tool backend unavailable"}), 502

    # If upstream is not JSON, pass-through
    try:
        payload_full = upstream_full.json()
    except Exception:
        resp = make_response(upstream_full.content, upstream_full.status_code)
        ct = upstream_full.headers.get("Content-Type")
        if ct:
            resp.headers["Content-Type"] = ct
        return resp

    items_full, wrap_full = _extract_list_container(payload_full)
    if items_full is None or not isinstance(items_full, list):
        return jsonify(payload_full), upstream_full.status_code

    # Start candidate pool with what we already have
    candidates = list(items_full)

    # 2) If too few results, gather more candidates using upstream variants
    if SUGGEST_BROAD_MATCH and (len(items_full) < BROAD_MATCH_FALLBACK_MIN_RESULTS):
        variants = _build_upstream_query_variants(raw_q)
        max_extra_calls = max(0, BROAD_MATCH_MAX_UPSTREAM_CALLS - 1)

        # Keep original args, just change q param
        base_args = request.args.to_dict(flat=True)

        calls = 0
        for v in variants:
            if calls >= max_extra_calls:
                break
            args2 = dict(base_args)
            args2[q_key] = v
            r = _call_upstream_get(url, headers, args2)
            if r is None:
                continue
            try:
                payload_v = r.json()
            except Exception:
                continue
            items_v, _wrap_v = _extract_list_container(payload_v)
            if isinstance(items_v, list) and items_v:
                candidates.extend(items_v)
                calls += 1

    candidates = _dedup_keep_order(candidates)

    # 3) Local broad-match filter + rank
    # Keep anything that matches at least 1 token; rank by matched token count then quality.
    if (SUGGEST_BROAD_MATCH or always_rerank) and tokens:
        scored = []
        for it in candidates:
            text = _item_text(it)
            matched_count, quality = _match_stats(text, tokens, raw_q)
            if matched_count >= 1:
                scored.append((matched_count, quality, it))

        # If nothing matched locally, fall back to upstream as-is
        if scored:
            scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
            ranked = [it for _, __, it in scored]
        else:
            ranked = items_full
    else:
        ranked = items_full

    if BROAD_MATCH_MAX_MERGED_RESULTS > 0:
        ranked = ranked[:BROAD_MATCH_MAX_MERGED_RESULTS]

    out_payload = wrap_full(ranked) if wrap_full else ranked
    return jsonify(out_payload), upstream_full.status_code

def _broad_match_response_for_post(always_rerank: bool):
    """
    Broad match for POST endpoints (/api/search) if enabled:
      - Try upstream with original body
      - If too few, fan out to variant upstream queries to gather candidates
      - Locally rank so: more tokens matched => higher rank
    """
    body = request.get_json(force=False, silent=True)
    if not isinstance(body, dict):
        return _proxy_upstream("POST")

    q_key = _guess_query_key_from_body(body)
    raw_q = body.get(q_key) if isinstance(body.get(q_key), str) else ""
    tokens = _tokenize_query(raw_q)

    # If no query, just proxy
    if not (raw_q or "").strip():
        return _proxy_upstream("POST")

    url = _upstream_url(request.path)
    headers = _forward_headers()

    # 1) Original upstream call
    upstream_full = _call_upstream_post(url, headers, request.args, body)
    if upstream_full is None:
        return jsonify({"error": "Upstream accessory tool backend unavailable"}), 502

    try:
        payload_full = upstream_full.json()
    except Exception:
        resp = make_response(upstream_full.content, upstream_full.status_code)
        ct = upstream_full.headers.get("Content-Type")
        if ct:
            resp.headers["Content-Type"] = ct
        return resp

    items_full, wrap_full = _extract_list_container(payload_full)
    if items_full is None or not isinstance(items_full, list):
        return jsonify(payload_full), upstream_full.status_code

    candidates = list(items_full)

    # 2) If too few results, gather more via variant queries
    if SEARCH_BROAD_MATCH and (len(items_full) < BROAD_MATCH_FALLBACK_MIN_RESULTS):
        variants = _build_upstream_query_variants(raw_q)
        max_extra_calls = max(0, BROAD_MATCH_MAX_UPSTREAM_CALLS - 1)

        calls = 0
        for v in variants:
            if calls >= max_extra_calls:
                break
            body2 = dict(body)
            body2[q_key] = v
            r = _call_upstream_post(url, headers, request.args, body2)
            if r is None:
                continue
            try:
                payload_v = r.json()
            except Exception:
                continue
            items_v, _wrap_v = _extract_list_container(payload_v)
            if isinstance(items_v, list) and items_v:
                candidates.extend(items_v)
                calls += 1

    candidates = _dedup_keep_order(candidates)

    # 3) Local broad-match filter + rank
    if (SEARCH_BROAD_MATCH or always_rerank) and tokens:
        scored = []
        for it in candidates:
            text = _item_text(it)
            matched_count, quality = _match_stats(text, tokens, raw_q)
            if matched_count >= 1:
                scored.append((matched_count, quality, it))

        if scored:
            scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
            ranked = [it for _, __, it in scored]
        else:
            ranked = items_full
    else:
        ranked = items_full

    if BROAD_MATCH_MAX_MERGED_RESULTS > 0:
        ranked = ranked[:BROAD_MATCH_MAX_MERGED_RESULTS]

    out_payload = wrap_full(ranked) if wrap_full else ranked
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
    # Broad-match + ranking:
    # - single-token searches like "phili" or "hx999" now also benefit
    # - multi-token searches like "hx9991 toothbrush" rank higher when more tokens match
    if not SUGGEST_BROAD_MATCH and not SUGGEST_RERANK_ALWAYS:
        return _proxy_upstream("GET")
    return _broad_match_response_for_get(always_rerank=SUGGEST_RERANK_ALWAYS)

@limiter.limit(RATE_LIMIT_SEARCH)
@app.post("/api/search")
def api_search():
    # Optional broad-match for search (POST). Enable with SEARCH_BROAD_MATCH=true
    if not SEARCH_BROAD_MATCH and not SEARCH_RERANK_ALWAYS:
        return _proxy_upstream("POST")
    return _broad_match_response_for_post(always_rerank=SEARCH_RERANK_ALWAYS)

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
