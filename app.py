import os
import re
import time
from io import BytesIO
from urllib.parse import urlparse

import pandas as pd
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

# Rate limiting
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# ──────────────────────────────────────────────────────────────────────────────
# Env (Railway):
#   GITHUB_TOKEN           (required) read-only for data repo
#   DATA_REPO              (default: chrisyau96/accessory-tool_data)
#   DATA_PATH              (default: Accessory-Core-Master.xlsx)
#   DATA_SHEET             (optional) e.g. Export Worksheet
#   SKU_COLUMN             (optional) the exact column name holding the SKU
#   CACHE_TTL_SECONDS      (default: 1800)
#   FRONTEND_ORIGINS       (csv) e.g. https://www.fortress.com.hk,https://fortress.com.hk
#   API_REFRESH_TOKEN      (required for /api/refresh)
#   ENFORCE_ORIGIN         (default: true)
#   MAX_ITEM_NAMES         (default: 200)
#   MAX_SUGGESTIONS        (default: 20)
#   NUMBER_SEARCH_DELAY_MS (default: 250)
#   RATE_LIMIT_DEFAULT     (default: "200 per hour")
#   RATE_LIMIT_META        (default: "30/minute;1000/day")
#   RATE_LIMIT_SEARCH      (default: "20/minute;500/day")
#   RATE_LIMIT_REFRESH     (default: "5/hour;20/day")
#   RATE_LIMIT_SUGGEST     (default: "60/minute;1500/day")
# ──────────────────────────────────────────────────────────────────────────────

app = Flask(__name__)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
DATA_REPO = os.getenv("DATA_REPO", "chrisyau96/accessory-tool_data")
DATA_PATH = os.getenv("DATA_PATH", "Accessory-Core-Master.xlsx")
DATA_SHEET = os.getenv("DATA_SHEET", "").strip()
SKU_COLUMN = os.getenv("SKU_COLUMN", "").strip()
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "1800"))

# CORS allowlist
_frontend_origins = [o.strip() for o in os.getenv("FRONTEND_ORIGINS", "").split(",") if o.strip()]
if not _frontend_origins:
    CORS(app)  # permissive only for first run; set FRONTEND_ORIGINS in prod
else:
    CORS(app, resources={r"/api/*": {"origins": _frontend_origins}})

# Limiter
limiter = Limiter(key_func=get_remote_address, default_limits=[os.getenv("RATE_LIMIT_DEFAULT", "200 per hour")])
limiter.init_app(app)

# Cache
_CACHE = {"df": None, "ts": 0}

# Origin enforcement
ENFORCE_ORIGIN = os.getenv("ENFORCE_ORIGIN", "true").lower() == "true"
ALLOWED_ORIGINS = {o.lower() for o in _frontend_origins}
API_REFRESH_TOKEN = os.getenv("API_REFRESH_TOKEN", "")
MAX_ITEM_NAMES = int(os.getenv("MAX_ITEM_NAMES", "200"))
MAX_SUGGESTIONS = int(os.getenv("MAX_SUGGESTIONS", "20"))
NUMBER_SEARCH_DELAY_MS = int(os.getenv("NUMBER_SEARCH_DELAY_MS", "250"))

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
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
    # Always allow health
    if request.path == "/api/healthz":
        return

    # Allow /api/refresh if a valid Bearer token is present (server→server call)
    if request.path == "/api/refresh":
        auth = request.headers.get("Authorization", "")
        if API_REFRESH_TOKEN and auth == f"Bearer {API_REFRESH_TOKEN}":
            return  # skip origin check

    # Enforce Origin/Referer for all other API calls
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

# ──────────────────────────────────────────────────────────────────────────────
# Data access & normalization
# ──────────────────────────────────────────────────────────────────────────────
def _fetch_excel_bytes_from_github() -> bytes:
    if not GITHUB_TOKEN:
        raise RuntimeError("Server missing GITHUB_TOKEN")
    url = f"https://api.github.com/repos/{DATA_REPO}/contents/{DATA_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",
        "User-Agent": "accessory-tool",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.content

def _find_sku_col(df: pd.DataFrame) -> str | None:
    candidates = []
    if SKU_COLUMN:
        candidates.append(SKU_COLUMN)
    candidates += ["Item", "ITEM", "ITEM_NO", "ITEM_NUMBER", "ITEM_CODE", "SKU", "PRODUCT_NO"]
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _ensure_item_str(df: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    sku_col = _find_sku_col(df)
    if sku_col:
        df["Item_str"] = (
            df[sku_col].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(8)
        )
    return df, sku_col

def _build_display_name(brand: str, pname_en: str) -> str:
    """
    DISPLAY_NAME = BRAND_NAME_EN + ' ' + PRODUCT_NAME_EN,
    but if PRODUCT_NAME_EN already starts with BRAND_NAME_EN (case-insensitive),
    just return PRODUCT_NAME_EN (no duplication).
    """
    b = (brand or "").strip()
    p = (pname_en or "").strip()
    if not b:
        return p
    if not p:
        return b
    # If p already starts with brand (e.g., "3M KJ455F-6 Air Cleaner"), keep p
    if p.lower().startswith(b.lower() + " " ) or p.lower() == b.lower():
        return p
    return f"{b} {p}".strip()

def _ensure_display_name(df: pd.DataFrame) -> pd.DataFrame:
    b = df.get("BRAND_NAME_EN")
    p = df.get("PRODUCT_NAME_EN")
    if b is not None or p is not None:
        brand = b.fillna("").astype(str) if b is not None else ""
        pname = p.fillna("").astype(str) if p is not None else ""
        df["DISPLAY_NAME"] = [
            _build_display_name(brand.iloc[i], pname.iloc[i]) for i in range(len(df))
        ]
    else:
        df["DISPLAY_NAME"] = ""
    return df

def _post_load_normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only rows where BUNDLE_TYPE is Compatible / Consumable
    if "BUNDLE_TYPE" in df.columns:
        df = df[df["BUNDLE_TYPE"].isin(["Compatible", "Consumable"])].copy()
    # Build helper columns we rely on
    df, _ = _ensure_item_str(df)
    df = _ensure_display_name(df)
    return df

def load_df(force: bool = False) -> pd.DataFrame:
    now = time.time()
    if _CACHE["df"] is not None and not force and (now - _CACHE["ts"] < CACHE_TTL):
        return _CACHE["df"]
    content = _fetch_excel_bytes_from_github()
    read_kwargs = {"engine": "openpyxl"}
    if DATA_SHEET:
        read_kwargs["sheet_name"] = DATA_SHEET
    df = pd.read_excel(BytesIO(content), **read_kwargs)
    df = _post_load_normalize(df)
    _CACHE.update({"df": df, "ts": now})
    return df

# ──────────────────────────────────────────────────────────────────────────────
# Search helpers
# ──────────────────────────────────────────────────────────────────────────────
def extract_sku_from_url(url: str) -> str | None:
    for p in (r"variant=(\d{8})", r"/p/(\d{8})", r"/p/BP_(\d{8})"):
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None

def _get_display_name(row: pd.Series) -> str:
    return str(row.get("DISPLAY_NAME", "")).strip()

# ──────────────────────────────────────────────────────────────────────────────
# Payload building
# ──────────────────────────────────────────────────────────────────────────────
def build_result(df: pd.DataFrame, product_number: str):
    df, _ = _ensure_item_str(df)
    match = df[df["Item_str"] == product_number]
    if match.empty:
        return None, None, f"Product {product_number} not found."

    row = match.iloc[0]
    item_type = row.get("ITEM_TYPE")
    type_label = "Accessory" if item_type == "A" else "Core Item"
    bundle_group = row.get("BUNDLE_ID")

    # Related = items in same bundle, opposite type
    related_df = pd.DataFrame()
    if ("BUNDLE_ID" in df.columns) and ("ITEM_TYPE" in df.columns) and pd.notna(bundle_group):
        opposite_type = "C" if item_type == "A" else "A"
        related_df = df[(df["BUNDLE_ID"] == bundle_group) & (df["ITEM_TYPE"] == opposite_type)].copy()
        if "DISPLAY_NAME" in related_df.columns:
            related_df = related_df.drop_duplicates(subset=["DISPLAY_NAME"])
        sort_cols = [c for c in ["RRP", "DISPLAY_NAME"] if c in related_df.columns]
        if sort_cols:
            related_df = related_df.sort_values(by=sort_cols)

    # Build main result
    allow_val = row.get("ALLOW_TO_BUY")
    try:
        allow_to_buy = int(allow_val) == 1
    except Exception:
        allow_to_buy = str(allow_val).strip() == "1"

    display_name = _get_display_name(row)

    result = {
        "item": product_number,
        "item_name_retek": display_name,   # keep FE keys; value now DISPLAY_NAME
        "item_name": display_name,
        "brand": str(row.get("BRAND_NAME_EN", "")),
        "department": str(row.get("ITEM_DEPT_NAME", "")),
        "item_type": str(item_type),
        "type_label": type_label,
        "rrp": (float(row["RRP"]) if "RRP" in row and pd.notna(row["RRP"]) else None),
        "allow_to_buy": 1 if allow_to_buy else 0,
    }

    # Related list
    related_items = []
    if not related_df.empty:
        related_df, _ = _ensure_item_str(related_df)
        for _, r in related_df.iterrows():
            allow = r.get("ALLOW_TO_BUY")
            try:
                allow_flag = int(allow) == 1
            except Exception:
                allow_flag = str(allow).strip() == "1"

            related_items.append(
                {
                    "Department": str(r.get("ITEM_DEPT_NAME", "")),
                    "Brand": str(r.get("BRAND_NAME_EN", "")),
                    "Item Name (retek)": str(r.get("DISPLAY_NAME", "")),
                    "Item Name": str(r.get("DISPLAY_NAME", "")),
                    "RRP": (float(r["RRP"]) if pd.notna(r.get("RRP", None)) else None),
                    "Item": str(r.get("Item_str", "")),
                    "Allow To Buy": 1 if allow_flag else 0,
                }
            )

    return result, related_items, None

# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/api/healthz")
def health():
    return {"ok": True}

@limiter.limit(os.getenv("RATE_LIMIT_META", "30/minute;1000/day"))
@app.get("/api/meta")
def api_meta():
    """
    ?type=A|C&department=<name>&brand=<name>
    Returns types, departments, brands; item_names only when all 3 filters provided (capped).
    Uses DISPLAY_NAME for item names.
    """
    df = load_df()
    q_type = request.args.get("type", "").strip()
    q_dept = request.args.get("department", "").strip()
    q_brand = request.args.get("brand", "").strip()

    filtered = df.copy()
    if q_type and "ITEM_TYPE" in filtered.columns:
        filtered = filtered[filtered["ITEM_TYPE"] == q_type]
    if q_dept and "ITEM_DEPT_NAME" in filtered.columns:
        filtered = filtered[filtered["ITEM_DEPT_NAME"] == q_dept]
    if q_brand and "BRAND_NAME_EN" in filtered.columns:
        filtered = filtered[filtered["BRAND_NAME_EN"] == q_brand]

    name_col = "DISPLAY_NAME" if "DISPLAY_NAME" in filtered.columns else None

    types = sorted(df["ITEM_TYPE"].dropna().unique().tolist()) if "ITEM_TYPE" in df.columns else []
    departments = sorted(filtered["ITEM_DEPT_NAME"].dropna().unique().tolist()) if "ITEM_DEPT_NAME" in filtered.columns else []
    brands = sorted(filtered["BRAND_NAME_EN"].dropna().unique().tolist()) if "BRAND_NAME_EN" in filtered.columns else []

    item_names = []
    if name_col and q_type and q_dept and q_brand:
        item_names = pd.Series(filtered[name_col]).dropna().astype(str).unique().tolist()
        item_names = sorted(item_names)[:MAX_ITEM_NAMES]  # cap to reduce enumeration

    return jsonify({"types": types, "departments": departments, "brands": brands, "item_names": item_names})

@limiter.limit(os.getenv("RATE_LIMIT_SUGGEST", "60/minute;1500/day"))
@app.get("/api/suggest")
def api_suggest():
    """
    Suggest item names by token-prefix match on DISPLAY_NAME.
    Optional filters: type, department, brand.
    """
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"suggestions": []})
    ql = q.lower()

    df = load_df()
    q_type = request.args.get("type", "").strip()
    q_dept = request.args.get("department", "").strip()
    q_brand = request.args.get("brand", "").strip()

    filtered = df.copy()
    if q_type and "ITEM_TYPE" in filtered.columns:
        filtered = filtered[filtered["ITEM_TYPE"] == q_type]
    if q_dept and "ITEM_DEPT_NAME" in filtered.columns:
        filtered = filtered[filtered["ITEM_DEPT_NAME"] == q_dept]
    if q_brand and "BRAND_NAME_EN" in filtered.columns:
        filtered = filtered[filtered["BRAND_NAME_EN"] == q_brand]

    name_col = "DISPLAY_NAME" if "DISPLAY_NAME" in filtered.columns else None
    if not name_col:
        return jsonify({"suggestions": []})

    names = pd.Series(filtered[name_col]).dropna().astype(str).unique().tolist()

    # allow English + CJK token starts
    def token_prefix_match(name: str) -> bool:
        tokens = re.split(r"[^A-Za-z0-9\u4e00-\u9fff]+", name.lower())
        return any(t.startswith(ql) for t in tokens if t)

    matched = [n for n in names if token_prefix_match(n)]
    matched = sorted(matched)[:MAX_SUGGESTIONS]
    return jsonify({"suggestions": matched})

@limiter.limit(os.getenv("RATE_LIMIT_SEARCH", "20/minute;500/day"))
@app.post("/api/search")
def api_search():
    """
    { action: "dropdown"|"link"|"number",
      selected_item_name?: string,      # DISPLAY_NAME
      product_link?: string,
      product_number?: string }
    """
    payload = request.get_json(force=True, silent=True) or {}
    action = (payload.get("action") or "").strip()

    df = load_df()
    product_number = None
    error = None

    if action == "dropdown":
        name = (payload.get("selected_item_name") or "").strip()
        if not name:
            error = "Please select the product."
        else:
            if "DISPLAY_NAME" in df.columns:
                match = df[df["DISPLAY_NAME"] == name]
            else:
                match = pd.DataFrame()
            if match.empty:
                error = "No match found for the selected product."
            else:
                sku_col = _find_sku_col(df)
                if not sku_col:
                    error = "SKU column not found in data."
                else:
                    sku = str(match.iloc[0].get(sku_col, "")).replace(".0", "")
                    product_number = sku.zfill(8)

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

    result, related, err = build_result(df, product_number)
    if err:
        return jsonify({"error": err}), 404

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

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)