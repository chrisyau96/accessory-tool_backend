import os
import re
import time
from io import BytesIO
from urllib.parse import urlparse

import pandas as pd
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)

# ── Config / Env ─────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
DATA_REPO = os.getenv("DATA_REPO", "chrisyau96/core-accessory-tool-data")
DATA_PATH = os.getenv("DATA_PATH", "Accessory-Core-Master.xlsx")
DATA_SHEET = os.getenv("DATA_SHEET", "").strip() or None
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "1800"))

_frontend_origins = [o.strip() for o in os.getenv("FRONTEND_ORIGINS", "").split(",") if o.strip()]
if not _frontend_origins:
    CORS(app)
else:
    CORS(app, resources={r"/api/*": {"origins": _frontend_origins}})

limiter = Limiter(key_func=get_remote_address, default_limits=[os.getenv("RATE_LIMIT_DEFAULT", "200 per hour")])
limiter.init_app(app)

_CACHE = {"df": None, "ts": 0}

ENFORCE_ORIGIN = os.getenv("ENFORCE_ORIGIN", "true").lower() == "true"
ALLOWED_ORIGINS = {o.lower() for o in _frontend_origins}
API_REFRESH_TOKEN = os.getenv("API_REFRESH_TOKEN", "")
MAX_ITEM_NAMES = int(os.getenv("MAX_ITEM_NAMES", "200"))
MAX_SUGGESTIONS = int(os.getenv("MAX_SUGGESTIONS", "20"))
NUMBER_SEARCH_DELAY_MS = int(os.getenv("NUMBER_SEARCH_DELAY_MS", "250"))

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
    resp.headers["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none'; base-uri 'none'"
    resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return resp

# ── Column helpers ───────────────────────────────────────────────────────────
def _get_series(row, *names):
    for n in names:
        if n in row:
            return row.get(n)
    return None

def _get_col(df, *names):
    for n in names:
        if n in df.columns:
            return n
    return None

# ── Data access ──────────────────────────────────────────────────────────────
def _fetch_excel_bytes_from_github() -> bytes:
    if not GITHUB_TOKEN:
        raise RuntimeError("Server missing GITHUB_TOKEN")
    url = f"https://api.github.com/repos/{DATA_REPO}/contents/{DATA_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",
        "User-Agent": "core-accessory-tool",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.content

def _post_load_normalize(df: pd.DataFrame) -> pd.DataFrame:
    if "BUNDLE_TYPE" in df.columns:
        df = df[df["BUNDLE_TYPE"].isin(["Compatible", "Consumable"])].copy()
    item_col = _get_col(df, "ITEM", "Item")
    if item_col:
        df["Item_str"] = (
            df[item_col].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(8)
        )
    return df

def load_df(force: bool = False) -> pd.DataFrame:
    now = time.time()
    if _CACHE["df"] is not None and not force and (now - _CACHE["ts"] < CACHE_TTL):
        return _CACHE["df"]
    content = _fetch_excel_bytes_from_github()
    if DATA_SHEET:
        df = pd.read_excel(BytesIO(content), engine="openpyxl", sheet_name=DATA_SHEET)
    else:
        df = pd.read_excel(BytesIO(content), engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    df = _post_load_normalize(df)
    _CACHE.update({"df": df, "ts": now})
    return df

def extract_sku_from_url(url: str) -> str | None:
    for p in (r"variant=(\d{8})", r"/p/(\d{8})", r"/p/BP_(\d{8})"):
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None

# ── Language helpers ─────────────────────────────────────────────────────────
LANG_BRAND_MAP = {"en": "BRAND_NAME_EN", "tc": "BRAND_NAME_TC", "sc": "BRAND_NAME_SC"}
LANG_PRODUCT_MAP = {"en": "PRODUCT_NAME_EN", "tc": "PRODUCT_NAME_TC", "sc": "PRODUCT_NAME_SC"}

def _norm_lang(s: str | None) -> str:
    s = (s or "en").lower()
    return "tc" if s == "zh-hk" else "sc" if s == "zh-cn" else ("en" if s not in ("en","tc","sc") else s)

def _safe_str(v) -> str:
    """Return '' for NaN/None, else str(v)."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v)

def _cap_words_en(brand: str) -> str:
    if not brand: 
        return brand
    if re.search(r"[^\x00-\x7F]", brand):
        return brand
    return re.sub(r"[A-Za-z]+", lambda m: m.group(0)[0].upper() + m.group(0)[1:].lower(), brand)

def _display_name(brand, product, lang: str) -> str:
    b = _safe_str(brand).strip()
    p = _safe_str(product).strip()
    if not b and not p:
        return ""
    if lang == "en":
        b_disp = _cap_words_en(b)
        bl = b.lower()
        pl = p.lower()
        if pl.startswith(bl + " "):
            return b_disp + p[len(b):]
        if pl == bl:
            return b_disp
        return f"{b_disp} {p}".strip()
    if p.startswith(b + " "):
        return b + p[len(b):]
    if p == b:
        return b
    return f"{b} {p}".strip()

def _brand_for_display(brand, lang: str) -> str:
    b = _safe_str(brand)
    return _cap_words_en(b) if lang == "en" else b

def _allow_to_buy_val(val) -> int:
    try:
        return 1 if int(val) == 1 else 0
    except Exception:
        return 1 if str(val).strip() == "1" else 0

# ── Column selection usable for DataFrame *or* Series ────────────────────────
def _select_cols(df_like, lang: str):
    if hasattr(df_like, "columns"):
        cols = set(map(str, df_like.columns))
    elif hasattr(df_like, "index"):
        cols = set(map(str, df_like.index))
    else:
        cols = set()

    def has(c): return c in cols

    brand_col = LANG_BRAND_MAP[lang] if has(LANG_BRAND_MAP[lang]) else None
    product_col = LANG_PRODUCT_MAP[lang] if has(LANG_PRODUCT_MAP[lang]) else None
    item_col = "ITEM" if has("ITEM") else ("Item" if has("Item") else None)
    dept_col = "ITEM_DEPT_NAME" if has("ITEM_DEPT_NAME") else None
    type_col = "ITEM_TYPE" if has("ITEM_TYPE") else None
    bundle_col = "BUNDLE_ID" if has("BUNDLE_ID") else None
    allow_col = "ALLOW_TO_BUY" if has("ALLOW_TO_BUY") else ("Allow To Buy" if has("Allow To Buy") else None)
    rrp_col = "RRP" if has("RRP") else None
    return brand_col, product_col, item_col, dept_col, type_col, bundle_col, allow_col, rrp_col

# ── Builders ─────────────────────────────────────────────────────────────────
def _match_by_display_name(df: pd.DataFrame, name: str, lang: str) -> pd.DataFrame:
    brand_col, product_col, *_ = _select_cols(df, lang)
    if not (brand_col and product_col):
        return pd.DataFrame()
    tmp = df[[c for c in (brand_col, product_col) if c in df.columns]].copy()
    tmp["_disp"] = [
        _display_name(b, p, lang) for b, p in zip(tmp.get(brand_col), tmp.get(product_col))
    ]
    return df[tmp["_disp"] == name]

def _row_to_result(row: pd.Series, lang: str) -> dict:
    brand_col, product_col, item_col, dept_col, type_col, bundle_col, allow_col, rrp_col = _select_cols(row, lang)
    brand_raw = _safe_str(_get_series(row, brand_col))
    product_raw = _safe_str(_get_series(row, product_col))
    dept = _safe_str(_get_series(row, dept_col))
    item_type = _safe_str(_get_series(row, type_col))
    type_label = "Accessory" if item_type == "A" else "Core Item"
    item = _safe_str(_get_series(row, "Item_str"))
    allow = _allow_to_buy_val(_get_series(row, allow_col) if allow_col else None)

    rrp = None
    rrp_cell = _get_series(row, "RRP")
    if rrp_cell is not None and pd.notna(rrp_cell):
        try:
            rrp = float(rrp_cell)
        except Exception:
            rrp = None

    brand_disp = _brand_for_display(brand_raw, lang)
    item_name_disp = _display_name(brand_raw, product_raw, lang)

    return {
        "item": item,
        "item_name_retek": item_name_disp,
        "item_name": item_name_disp,
        "brand": brand_disp,
        "department": dept,
        "item_type": item_type,
        "type_label": type_label,
        "rrp": rrp,
        "allow_to_buy": allow,
    }

def _related_items(df: pd.DataFrame, row: pd.Series, lang: str):
    brand_col, product_col, item_col, dept_col, type_col, bundle_col, allow_col, rrp_col = _select_cols(df, lang)
    if not (bundle_col and type_col) or pd.isna(row.get(bundle_col)):
        return []

    opposite_type = "C" if row.get(type_col) == "A" else "A"
    rel = df[(df[bundle_col] == row.get(bundle_col)) & (df[type_col] == opposite_type)].copy()

    rel["_disp"] = [
        _display_name(_safe_str(r.get(brand_col, "")), _safe_str(r.get(product_col, "")), lang)
        for _, r in rel.iterrows()
    ]
    rel = rel.drop_duplicates(subset=["_disp"])

    sort_cols = [c for c in ["RRP", "_disp"] if c in rel.columns]
    if sort_cols:
        rel = rel.sort_values(by=sort_cols)

    if "Item_str" not in rel.columns and item_col in rel.columns:
        rel["Item_str"] = (
            rel[item_col].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(8)
        )

    out = []
    for _, r in rel.iterrows():
        allow = _allow_to_buy_val(r.get(allow_col) if allow_col else None)
        rrp_val = None
        if "RRP" in r and pd.notna(r["RRP"]):
            try:
                rrp_val = float(r["RRP"])
            except Exception:
                rrp_val = None

        out.append({
            "Department": _safe_str(r.get(dept_col, "")),
            "Brand": _brand_for_display(_safe_str(r.get(brand_col, "")), lang),
            "Item Name (retek)": _safe_str(r.get("_disp", "")),
            "Item Name": _safe_str(r.get("_disp", "")),
            "RRP": rrp_val,
            "Item": _safe_str(r.get("Item_str", "")),
            "Allow To Buy": 1 if allow else 0,
        })
    return out

# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/api/healthz")
def health():
    return {"ok": True}

@limiter.limit(os.getenv("RATE_LIMIT_META", "30/minute;1000/day"))
@app.get("/api/meta")
def api_meta():
    df = load_df()
    lang = _norm_lang(request.args.get("lang"))
    q_type = (request.args.get("type") or "").strip()
    q_dept = (request.args.get("department") or "").strip()
    q_brand = (request.args.get("brand") or "").strip()

    brand_col, product_col, item_col, dept_col, type_col, bundle_col, allow_col, rrp_col = _select_cols(df, lang)

    filtered = df.copy()
    if q_type and type_col in filtered.columns:
        filtered = filtered[filtered[type_col] == q_type]
    if q_dept and dept_col in filtered.columns:
        filtered = filtered[filtered[dept_col] == q_dept]
    if q_brand and brand_col in filtered.columns:
        filtered = filtered[filtered[brand_col] == q_brand]

    types = sorted(df[type_col].dropna().unique().tolist()) if type_col in df.columns else []
    departments = sorted(filtered[dept_col].dropna().astype(str).unique().tolist()) if dept_col in filtered.columns else []
    brands = sorted(filtered[brand_col].dropna().astype(str).unique().tolist()) if brand_col in filtered.columns else []

    item_names = []
    if brand_col and product_col and q_type and q_dept and q_brand:
        disp = [
            _display_name(b, p, lang)
            for b, p in zip(filtered.get(brand_col), filtered.get(product_col))
        ]
        item_names = sorted(pd.Series(disp).dropna().astype(str).unique().tolist())[:MAX_ITEM_NAMES]

    return jsonify({"types": types, "departments": departments, "brands": brands, "item_names": item_names})

@limiter.limit(os.getenv("RATE_LIMIT_SUGGEST", "60/minute;1500/day"))
@app.get("/api/suggest")
def api_suggest():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"suggestions": []})
    ql = q.lower()

    df = load_df()
    lang = _norm_lang(request.args.get("lang"))
    brand_col, product_col, item_col, dept_col, type_col, bundle_col, allow_col, rrp_col = _select_cols(df, lang)

    q_type = (request.args.get("type") or "").strip()
    q_dept = (request.args.get("department") or "").strip()
    q_brand = (request.args.get("brand") or "").strip()

    filtered = df.copy()
    if q_type and type_col in filtered.columns:
        filtered = filtered[filtered[type_col] == q_type]
    if q_dept and dept_col in filtered.columns:
        filtered = filtered[filtered[dept_col] == q_dept]
    if q_brand and brand_col in filtered.columns:
        filtered = filtered[filtered[brand_col] == q_brand]

    if not (brand_col and product_col):
        return jsonify({"suggestions": []})

    names = [
        _display_name(b, p, lang)
        for b, p in zip(filtered.get(brand_col), filtered.get(product_col))
    ]
    names = pd.Series(names).dropna().astype(str).unique().tolist()

    def token_prefix_match(name: str) -> bool:
        tokens = re.split(r"[^A-Za-z0-9\u4e00-\u9fff\u3400-\u4dbf]+", name.lower())
        return any(t.startswith(ql.lower()) for t in tokens if t)

    matched = [n for n in names if token_prefix_match(n)]
    matched = sorted(matched)[:MAX_SUGGESTIONS]
    return jsonify({"suggestions": matched})

@limiter.limit(os.getenv("RATE_LIMIT_SEARCH", "20/minute;500/day"))
@app.post("/api/search")
def api_search():
    payload = request.get_json(force=True, silent=True) or {}
    action = (payload.get("action") or "").strip()
    lang = _norm_lang(payload.get("lang"))

    df = load_df()
    product_number = None
    error = None

    if action == "dropdown":
        name = (payload.get("selected_item_name") or "").strip()
        if not name:
            error = "Please select the product."
        else:
            match = _match_by_display_name(df, name, lang)
            if match.empty:
                error = "No match found for the selected product."
            else:
                item_col = _get_col(match, "ITEM", "Item")
                sku_raw = str(match.iloc[0].get(item_col, "")).replace(".0", "") if item_col else ""
                product_number = sku_raw.zfill(8)

    elif action == "link":
        link = (payload.get("product_link") or "").strip()
        if not (link.startswith("http://") or link.startswith("https://")):
            error = "Please enter a valid product link."
        else:
            sku = extract_sku_from_url(link)
            if sku:
                product_number = sku
            else:
                error = "Please enter a valid product link."

    elif action == "number":
        if NUMBER_SEARCH_DELAY_MS > 0:
            time.sleep(NUMBER_SEARCH_DELAY_MS / 1000.0)
        num = (payload.get("product_number") or "").strip()
        if not num.isdigit() or len(num) != 8:
            error = "Please enter an 8-digit product number."
        else:
            product_number = num
    else:
        error = "Invalid action."

    if error:
        return jsonify({"error": error}), 400

    if "Item_str" not in df.columns:
        item_col = _get_col(df, "ITEM", "Item")
        if item_col:
            df["Item_str"] = (
                df[item_col].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(8)
            )
    match = df[df["Item_str"] == product_number]
    if match.empty:
        return jsonify({"error": f"Product {product_number} not found."}), 404

    row = match.iloc[0]
    result = _row_to_result(row, lang)
    related = _related_items(df, row, lang)
    return jsonify({"result": result, "related_items": related})

@limiter.limit(os.getenv("RATE_LIMIT_REFRESH", "5/hour;20/day"))
@app.post("/api/refresh")
def api_refresh():
    auth = request.headers.get("Authorization", "")
    if not API_REFRESH_TOKEN or auth != f"Bearer {API_REFRESH_TOKEN}":
        return jsonify({"error": "unauthorized"}), 401
    load_df(force=True)
    return jsonify({"ok": True, "message": "Data cache refreshed."})

@app.errorhandler(429)
def _ratelimit_handler(e):
    return jsonify({"error": "Too many requests. Please try again later."}), 429

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)