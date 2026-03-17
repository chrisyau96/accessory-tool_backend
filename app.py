import os
import re
import time
import logging
import threading
from io import BytesIO
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("core-accessory-tool")

ALL_LANGS = ("en", "tc", "sc")


# ── Helpers ──────────────────────────────────────────────────────────────────
def _normalize_origin(value: str) -> str:
    try:
        p = urlparse((value or "").strip())
        if p.scheme and p.netloc:
            return f"{p.scheme.lower()}://{p.netloc.lower()}"
    except Exception:
        pass
    return ""


def _safe_str(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v)


def _clean_text_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _normalize_display_text(s: str) -> str:
    return re.sub(r"\s+", " ", _safe_str(s)).strip().casefold()


def _get_col(df, *names):
    for n in names:
        if n in df.columns:
            return n
    return None


def _get_series(row, *names):
    for n in names:
        if n and n in row:
            return row.get(n)
    return None


def _tokenize_search_text(s: str) -> list[str]:
    return [
        t
        for t in re.split(r"[^A-Za-z0-9\u4e00-\u9fff\u3400-\u4dbf]+", (s or "").lower())
        if t
    ]


def _compact_search_text(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9\u4e00-\u9fff\u3400-\u4dbf]+", "", (s or "").lower())


# ── Config ───────────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
DATA_REPO = os.getenv("DATA_REPO", "chrisyau96/core-accessory-tool-data").strip()

DATA_PATH = os.getenv("DATA_PATH", "Accessory-Core-Master.xlsx").strip()
DATA_SHEET = os.getenv("DATA_SHEET", "").strip() or None
DEPT_MAP_PATH = os.getenv("DEPT_MAP_PATH", "mapping.xlsx").strip()

CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "1800"))
API_REFRESH_TOKEN = os.getenv("API_REFRESH_TOKEN", "").strip()
ENFORCE_ORIGIN = os.getenv("ENFORCE_ORIGIN", "true").lower() == "true"

MAX_ITEM_NAMES = int(os.getenv("MAX_ITEM_NAMES", "200"))
MAX_SUGGESTIONS = int(os.getenv("MAX_SUGGESTIONS", "20"))
NUMBER_SEARCH_DELAY_MS = int(os.getenv("NUMBER_SEARCH_DELAY_MS", "250"))
SCROLL_OFFSET_PX = int(os.getenv("SCROLL_OFFSET_PX", "160"))

_raw_frontend_origins = [
    o.strip() for o in os.getenv("FRONTEND_ORIGINS", "").split(",") if o.strip()
]
FRONTEND_ORIGINS = [o for o in (_normalize_origin(x) for x in _raw_frontend_origins) if o]
ALLOWED_ORIGINS = set(FRONTEND_ORIGINS)

if FRONTEND_ORIGINS:
    CORS(app, resources={r"/api/*": {"origins": FRONTEND_ORIGINS}})
else:
    CORS(app, resources={r"/api/*": {"origins": "*"}})

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[os.getenv("RATE_LIMIT_DEFAULT", "200 per hour")],
)
limiter.init_app(app)

_CACHE = {"df": None, "ts": 0}
_CACHE_LOCK = threading.Lock()


# ── HTTP session ─────────────────────────────────────────────────────────────
_HTTP = requests.Session()
_retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"]),
)
_adapter = HTTPAdapter(max_retries=_retry)
_HTTP.mount("https://", _adapter)
_HTTP.mount("http://", _adapter)


# ── Security ─────────────────────────────────────────────────────────────────
def _origin_allowed() -> bool:
    if not ENFORCE_ORIGIN or not ALLOWED_ORIGINS:
        return True

    origin = request.headers.get("Origin", "")
    ref = request.headers.get("Referer", "")

    if origin:
        return _normalize_origin(origin) in ALLOWED_ORIGINS

    if ref:
        return _normalize_origin(ref) in ALLOWED_ORIGINS

    return True


@app.before_request
def _block_unknown_origins():
    if request.path == "/api/healthz":
        return

    if request.path == "/api/refresh":
        auth = request.headers.get("Authorization", "")
        if API_REFRESH_TOKEN and auth == f"Bearer {API_REFRESH_TOKEN}":
            return

    if request.path.startswith("/api/") and not _origin_allowed():
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


# ── Data loading ─────────────────────────────────────────────────────────────
def _ensure_minimum_columns(df: pd.DataFrame):
    required = {
        "BUNDLE_TYPE",
        "BUNDLE_ID",
        "ITEM",
        "EXCLUSION",
        "ITEM_DEPT_NAME",
        "BRAND_NAME_EN",
        "BRAND_NAME_TC",
        "BRAND_NAME_SC",
        "PRODUCT_NAME_EN",
        "PRODUCT_NAME_TC",
        "PRODUCT_NAME_SC",
        "RRP",
        "ALLOW_TO_BUY",
    }
    missing = sorted(c for c in required if c not in df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def _fetch_excel_bytes_from_github_path(path: str) -> bytes:
    if not GITHUB_TOKEN:
        raise RuntimeError("Server missing GITHUB_TOKEN")

    url = f"https://api.github.com/repos/{DATA_REPO}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",
        "User-Agent": "core-accessory-tool",
    }

    r = _HTTP.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.content


def _post_load_normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    _ensure_minimum_columns(df)

    bundle_type = _clean_text_series(df["BUNDLE_TYPE"]).str.lower()
    df = df[bundle_type == "consumable"].copy()

    exclusion = _clean_text_series(df["EXCLUSION"]).str.upper()
    df = df[exclusion != "Y"].copy()

    item_col = _get_col(df, "ITEM", "Item")
    if item_col:
        df["Item_str"] = (
            df[item_col]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.replace(r"\D", "", regex=True)
            .str.strip()
            .str.zfill(8)
        )

    trim_cols = [
        "SHEET_NAME",
        "ITEM_DEPT_NAME",
        "BRAND_NAME_EN", "BRAND_NAME_TC", "BRAND_NAME_SC",
        "PRODUCT_NAME_EN", "PRODUCT_NAME_TC", "PRODUCT_NAME_SC",
    ]
    for c in trim_cols:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()

    return df


def _load_dept_mapping() -> pd.DataFrame:
    try:
        content = _fetch_excel_bytes_from_github_path(DEPT_MAP_PATH)
        dfm = pd.read_excel(BytesIO(content), engine="openpyxl")
    except Exception:
        logger.exception("Unable to load department mapping: %s", DEPT_MAP_PATH)
        dfm = pd.DataFrame()

    dfm.columns = [str(c).strip() for c in dfm.columns]

    for c in ["ITEM_DEPT_NAME", "ITEM_DEPT_NAME_EN", "ITEM_DEPT_NAME_TC", "ITEM_DEPT_NAME_SC"]:
        if c not in dfm.columns:
            dfm[c] = ""

    if not dfm.empty:
        dfm["ITEM_DEPT_NAME"] = dfm["ITEM_DEPT_NAME"].astype(str).str.strip()
        dfm = dfm.drop_duplicates(subset=["ITEM_DEPT_NAME"], keep="first")

    return dfm


def load_df(force: bool = False) -> pd.DataFrame:
    now = time.time()

    with _CACHE_LOCK:
        if _CACHE["df"] is not None and not force and (now - _CACHE["ts"] < CACHE_TTL):
            return _CACHE["df"]

        content = _fetch_excel_bytes_from_github_path(DATA_PATH)

        if DATA_SHEET:
            df = pd.read_excel(BytesIO(content), engine="openpyxl", sheet_name=DATA_SHEET)
        else:
            df = pd.read_excel(BytesIO(content), engine="openpyxl")

        if not isinstance(df, pd.DataFrame):
            raise ValueError("Loaded workbook is not a single-sheet DataFrame. Please set DATA_SHEET correctly.")

        df.columns = [str(c).strip() for c in df.columns]
        df = _post_load_normalize(df)

        df_map = _load_dept_mapping()
        if "ITEM_DEPT_NAME" in df.columns and not df_map.empty:
            df["ITEM_DEPT_NAME"] = df["ITEM_DEPT_NAME"].astype(str).str.strip()
            df = df.merge(df_map, on="ITEM_DEPT_NAME", how="left")

        _CACHE.update({"df": df, "ts": now})
        logger.info("Dataset loaded: %s rows after filtering", len(df))
        return df


def extract_sku_from_url(url: str) -> str | None:
    value = (url or "").strip()
    if not value:
        return None

    try:
        parsed = urlparse(value)
        qs = parse_qs(parsed.query)

        for key in ("variant", "sku", "item", "product", "productId"):
            for raw in qs.get(key, []):
                m = re.search(r"(\d{8})", raw)
                if m:
                    return m.group(1)
    except Exception:
        pass

    patterns = (
        r"/p/BP_(\d{8})(?:[/?#]|$)",
        r"/p/(\d{8})(?:[/?#]|$)",
        r"(?:sku|item|variant|productId|product)[=/](\d{8})(?:[&#/?]|$)",
        r"(?<!\d)(\d{8})(?!\d)",
    )

    for p in patterns:
        m = re.search(p, value, flags=re.IGNORECASE)
        if m:
            return m.group(1)

    return None


# ── Language helpers ─────────────────────────────────────────────────────────
LANG_BRAND_MAP = {
    "en": "BRAND_NAME_EN",
    "tc": "BRAND_NAME_TC",
    "sc": "BRAND_NAME_SC",
}
LANG_PRODUCT_MAP = {
    "en": "PRODUCT_NAME_EN",
    "tc": "PRODUCT_NAME_TC",
    "sc": "PRODUCT_NAME_SC",
}


def _norm_lang(s: str | None) -> str:
    s = (s or "en").strip().lower().replace("_", "-")
    if s in ("zh-hk", "tc"):
        return "tc"
    if s in ("zh-cn", "sc"):
        return "sc"
    if s in ("en", "tc", "sc"):
        return s
    return "en"


def _cap_words_en(brand: str) -> str:
    if not brand:
        return brand
    if re.search(r"[^\x00-\x7F]", brand):
        return brand
    return re.sub(
        r"[A-Za-z]+",
        lambda m: m.group(0)[0].upper() + m.group(0)[1:].lower(),
        brand,
    )


def _display_name(brand, product, lang: str) -> str:
    b = _safe_str(brand).strip()
    p = _safe_str(product).strip()

    if not b and not p:
        return ""

    if lang == "en":
        b_disp = _cap_words_en(b)
        bl = b.lower()
        pl = p.lower()

        if bl and pl.startswith(bl + " "):
            return b_disp + p[len(b):]
        if pl == bl:
            return b_disp
        return f"{b_disp} {p}".strip()

    if b and p.startswith(b + " "):
        return b + p[len(b):]
    if p == b:
        return b
    return f"{b} {p}".strip()


def _display_name_for_lang(row_like, lang: str) -> str:
    brand_col = LANG_BRAND_MAP.get(lang)
    product_col = LANG_PRODUCT_MAP.get(lang)
    brand = _safe_str(row_like.get(brand_col)).strip() if hasattr(row_like, "get") else ""
    product = _safe_str(row_like.get(product_col)).strip() if hasattr(row_like, "get") else ""
    return _display_name(brand, product, lang).strip()


def _brand_for_display(brand, lang: str) -> str:
    b = _safe_str(brand).strip()
    return _cap_words_en(b) if lang == "en" else b


def _lang_order(preferred_lang: str):
    return [preferred_lang] + [l for l in ALL_LANGS if l != preferred_lang]


def _display_name_fallback(row_like, preferred_lang: str) -> str:
    for lang in _lang_order(preferred_lang):
        disp = _display_name_for_lang(row_like, lang)
        if disp:
            return disp
    return ""


def _brand_fallback(row_like, preferred_lang: str) -> str:
    for lang in _lang_order(preferred_lang):
        brand_col = LANG_BRAND_MAP.get(lang)
        brand = _safe_str(row_like.get(brand_col)).strip() if hasattr(row_like, "get") else ""
        if brand:
            return _brand_for_display(brand, lang)
    return ""


def _allow_to_buy_val(val) -> int:
    try:
        return 1 if int(val) == 1 else 0
    except Exception:
        return 1 if str(val).strip() == "1" else 0


def _dept_label(row_like, lang: str) -> str:
    col = {
        "en": "ITEM_DEPT_NAME_EN",
        "tc": "ITEM_DEPT_NAME_TC",
        "sc": "ITEM_DEPT_NAME_SC",
    }[lang]

    lbl = _safe_str(row_like.get(col)) if hasattr(row_like, "get") else ""
    if lbl:
        return lbl

    return _safe_str(row_like.get("ITEM_DEPT_NAME")) if hasattr(row_like, "get") else ""


def _normalize_type_value(v: str) -> str:
    x = (v or "").strip().upper()

    if x in ("A", "ACCESSORY"):
        return "A"
    if x in ("C", "CORE", "CORE ITEM"):
        return "C"

    if "ACCESS" in x:
        return "A"
    if "CORE" in x:
        return "C"

    return x


def _apply_type_filter(df: pd.DataFrame, q_type: str, type_col: str | None) -> pd.DataFrame:
    if not q_type or not type_col or type_col not in df.columns:
        return df

    q_norm = _normalize_type_value(q_type)
    ser = _clean_text_series(df[type_col]).str.upper().map(_normalize_type_value)

    if q_norm in ("A", "C"):
        return df[ser == q_norm]

    return df[ser == q_norm]


def _brand_mask(df: pd.DataFrame, q_brand: str) -> pd.Series:
    if not q_brand:
        return pd.Series([True] * len(df), index=df.index)

    q_brand_norm = q_brand.strip().lower()
    masks = []

    for lang in ALL_LANGS:
        bcol = LANG_BRAND_MAP.get(lang)
        if bcol and bcol in df.columns:
            masks.append(df[bcol].fillna("").astype(str).str.strip().str.lower() == q_brand_norm)

    if not masks:
        return pd.Series([True] * len(df), index=df.index)

    return pd.concat(masks, axis=1).any(axis=1)


def _collect_search_values(row_like) -> list[str]:
    vals = []

    for lang in ALL_LANGS:
        brand_col = LANG_BRAND_MAP.get(lang)
        product_col = LANG_PRODUCT_MAP.get(lang)

        brand = _safe_str(row_like.get(brand_col)).strip() if hasattr(row_like, "get") else ""
        product = _safe_str(row_like.get(product_col)).strip() if hasattr(row_like, "get") else ""

        if brand:
            vals.append(brand)
        if product:
            vals.append(product)

        disp = _display_name(brand, product, lang).strip()
        if disp:
            vals.append(disp)

    item_str = _safe_str(row_like.get("Item_str")).strip() if hasattr(row_like, "get") else ""
    if item_str:
        vals.append(item_str)

    seen = set()
    out = []
    for v in vals:
        key = _normalize_display_text(v)
        if v and key not in seen:
            seen.add(key)
            out.append(v)
    return out


def _select_cols(df_like, lang: str):
    if hasattr(df_like, "columns"):
        cols = set(map(str, df_like.columns))
    elif hasattr(df_like, "index"):
        cols = set(map(str, df_like.index))
    else:
        cols = set()

    def has(c):
        return c in cols

    brand_col = LANG_BRAND_MAP[lang] if has(LANG_BRAND_MAP[lang]) else None
    product_col = LANG_PRODUCT_MAP[lang] if has(LANG_PRODUCT_MAP[lang]) else None
    item_col = "ITEM" if has("ITEM") else ("Item" if has("Item") else None)
    dept_col = "ITEM_DEPT_NAME" if has("ITEM_DEPT_NAME") else None
    type_col = (
        "ITEM_TYPE" if has("ITEM_TYPE")
        else ("SHEET_NAME" if has("SHEET_NAME") else None)
    )
    bundle_col = "BUNDLE_ID" if has("BUNDLE_ID") else None
    allow_col = "ALLOW_TO_BUY" if has("ALLOW_TO_BUY") else ("Allow To Buy" if has("Allow To Buy") else None)
    rrp_col = "RRP" if has("RRP") else None

    return brand_col, product_col, item_col, dept_col, type_col, bundle_col, allow_col, rrp_col


# ── Matching / scoring ───────────────────────────────────────────────────────
def _match_by_display_name(df: pd.DataFrame, name: str, lang: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    target = _normalize_display_text(name)

    tmp = df.copy()
    tmp["_disp"] = [_display_name_fallback(r, lang) for _, r in tmp.iterrows()]
    tmp["_disp_key"] = tmp["_disp"].map(_normalize_display_text)

    return tmp[tmp["_disp_key"] == target]


def _broad_match(query: str, candidate: str) -> bool:
    q = (query or "").strip()
    cand = (candidate or "").strip()

    if not q or not cand:
        return False

    q_tokens = _tokenize_search_text(q)
    q_compact = _compact_search_text(q)

    cand_l = cand.lower()
    cand_tokens = _tokenize_search_text(cand_l)
    cand_compact = _compact_search_text(cand_l)

    if not q_tokens:
        return False

    if q_compact and q_compact in cand_compact:
        return True

    for qt in q_tokens:
        if qt in cand_l:
            continue

        qt_compact = _compact_search_text(qt)
        if qt_compact and qt_compact in cand_compact:
            continue

        if any(ct.startswith(qt) or qt in ct for ct in cand_tokens):
            continue

        return False

    return True


def _score_candidate_value(query: str, candidate: str) -> int:
    """
    Higher score = stronger / more phrase-like match.
    Keeps broad match flexibility, but ranks contiguous phrase matches much higher.
    """
    if not _broad_match(query, candidate):
        return -1

    q_raw = (query or "").strip().lower()
    c_raw = (candidate or "").strip().lower()

    q_norm = _normalize_display_text(query)
    c_norm = _normalize_display_text(candidate)

    q_tokens = _tokenize_search_text(query)
    c_tokens = _tokenize_search_text(candidate)

    q_compact = _compact_search_text(query)
    c_compact = _compact_search_text(candidate)

    score = 0

    # Full / phrase match priority
    if q_norm and c_norm == q_norm:
        score += 2000
    elif q_norm and c_norm.startswith(q_norm):
        score += 1600
    elif q_norm and q_norm in c_norm:
        score += 1300

    # Compact phrase match (handles spaces / punctuation differences)
    if q_compact and c_compact == q_compact:
        score += 1800
    elif q_compact and c_compact.startswith(q_compact):
        score += 1400
    elif q_compact and q_compact in c_compact:
        score += 1100

    # Earlier occurrence is better
    if q_compact and q_compact in c_compact:
        idx = c_compact.find(q_compact)
        score += max(0, 250 - min(idx, 250))

    if q_raw and q_raw in c_raw:
        idx2 = c_raw.find(q_raw)
        score += max(0, 180 - min(idx2, 180))

    # Token-level scoring
    for qt in q_tokens:
        if not qt:
            continue

        if any(ct == qt for ct in c_tokens):
            score += 260
        elif any(ct.startswith(qt) for ct in c_tokens):
            score += 180
        elif any(qt in ct for ct in c_tokens):
            score += 120

    # Shorter display strings with same match tend to be better
    score += max(0, 80 - min(len(c_compact), 80))

    return score


def _row_suggest_score(row: pd.Series, query: str, lang: str) -> int:
    """
    Rank phrase matches in display / product names above SKU-only matches,
    while still keeping broad match flexibility.
    """
    scores = []

    def push(text: str, weight: int):
        s = _score_candidate_value(query, text)
        if s >= 0:
            scores.append(weight + s)

    # Highest priority: current-lang display name
    display_current = _display_name_for_lang(row, lang)
    if display_current:
        push(display_current, 5000)

    # Fallback display name
    display_fallback = _display_name_fallback(row, lang)
    if display_fallback and _normalize_display_text(display_fallback) != _normalize_display_text(display_current):
        push(display_fallback, 4600)

    # Current language product / brand
    brand_col = LANG_BRAND_MAP.get(lang)
    product_col = LANG_PRODUCT_MAP.get(lang)
    push(_safe_str(row.get(product_col, "")).strip(), 4200)
    push(_safe_str(row.get(brand_col, "")).strip(), 3000)

    # Other languages still searchable, but rank lower
    for other_lang in ALL_LANGS:
        if other_lang == lang:
            continue
        push(_display_name_for_lang(row, other_lang), 2600)
        push(_safe_str(row.get(LANG_PRODUCT_MAP[other_lang], "")).strip(), 2200)
        push(_safe_str(row.get(LANG_BRAND_MAP[other_lang], "")).strip(), 1500)

    # SKU stays broad-matchable, but lower than name / display matches
    push(_safe_str(row.get("Item_str", "")).strip(), 900)

    return max(scores) if scores else -1


def _row_to_result(row: pd.Series, lang: str) -> dict:
    _, _, _, dept_col, type_col, _, allow_col, _ = _select_cols(row, lang)

    dept_raw = _safe_str(_get_series(row, dept_col))
    dept_disp = _dept_label(row, lang)

    item_type_raw = _safe_str(_get_series(row, type_col)).strip()
    item_type_norm = _normalize_type_value(item_type_raw)

    type_label = ""
    if item_type_norm == "A":
        type_label = "Accessory"
    elif item_type_norm == "C":
        type_label = "Core Item"

    item = _safe_str(_get_series(row, "Item_str"))
    allow = _allow_to_buy_val(_get_series(row, allow_col) if allow_col else None)

    rrp = None
    rrp_cell = _get_series(row, "RRP")
    if rrp_cell is not None and pd.notna(rrp_cell):
        try:
            rrp = float(rrp_cell)
        except Exception:
            rrp = None

    brand_disp = _brand_fallback(row, lang)
    item_name_disp = _display_name_fallback(row, lang)

    return {
        "item": item,
        "item_name_retek": item_name_disp,
        "item_name": item_name_disp,
        "brand": brand_disp,
        "department": dept_disp,
        "department_raw": dept_raw,
        "item_type": item_type_raw,
        "type_label": type_label,
        "rrp": rrp,
        "allow_to_buy": allow,
    }


def _related_items(df: pd.DataFrame, row: pd.Series, lang: str):
    _, _, item_col, _, type_col, bundle_col, allow_col, _ = _select_cols(df, lang)

    if not bundle_col or pd.isna(row.get(bundle_col)):
        return []

    row_item = _safe_str(row.get("Item_str")).strip()
    row_bundle = row.get(bundle_col)

    rel = df[df[bundle_col] == row_bundle].copy()

    if type_col and type_col in df.columns:
        row_type_norm = _normalize_type_value(_safe_str(row.get(type_col)))
        if row_type_norm == "A":
            rel = rel[_clean_text_series(rel[type_col]).str.upper().map(_normalize_type_value) == "C"].copy()
        elif row_type_norm == "C":
            rel = rel[_clean_text_series(rel[type_col]).str.upper().map(_normalize_type_value) == "A"].copy()

    if row_item:
        rel = rel[rel["Item_str"].astype(str).str.strip() != row_item].copy()

    rel["_disp"] = [_display_name_fallback(r, lang) for _, r in rel.iterrows()]
    rel = rel[rel["_disp"].astype(str).str.strip() != ""]
    rel = rel.drop_duplicates(subset=["Item_str"], keep="first")

    sort_cols = [c for c in ["RRP", "_disp"] if c in rel.columns]
    if sort_cols:
        rel = rel.sort_values(by=sort_cols)

    if "Item_str" not in rel.columns and item_col in rel.columns:
        rel["Item_str"] = (
            rel[item_col]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.replace(r"\D", "", regex=True)
            .str.strip()
            .str.zfill(8)
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
            "Department": _dept_label(r, lang),
            "Brand": _brand_fallback(r, lang),
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
    return {
        "ok": True,
        "repo": DATA_REPO,
        "data_path": DATA_PATH,
        "sheet": DATA_SHEET or "",
        "cache_ttl_seconds": CACHE_TTL,
        "allowed_origins": FRONTEND_ORIGINS,
    }


@limiter.limit(os.getenv("RATE_LIMIT_META", "30/minute;1000/day"))
@app.get("/api/meta")
def api_meta():
    try:
        df = load_df()
    except Exception:
        logger.exception("api_meta failed")
        return jsonify({"error": "dataset unavailable"}), 500

    lang = _norm_lang(request.args.get("lang"))
    q_type = (request.args.get("type") or "").strip()
    q_dept = (request.args.get("department") or "").strip()
    q_brand = (request.args.get("brand") or "").strip()

    _, _, _, dept_col, type_col, _, _, _ = _select_cols(df, lang)

    filtered = df.copy()
    filtered = _apply_type_filter(filtered, q_type, type_col)

    if q_dept and dept_col in filtered.columns:
        filtered = filtered[filtered[dept_col].fillna("").astype(str).str.strip() == q_dept]

    if q_brand:
        filtered = filtered[_brand_mask(filtered, q_brand)]

    types = []
    if type_col and type_col in df.columns:
        type_vals = _clean_text_series(df[type_col]).str.upper()
        normalized_types = []
        for v in type_vals.unique().tolist():
            nv = _normalize_type_value(v)
            if nv in ("A", "C") and nv not in normalized_types:
                normalized_types.append(nv)
        types = sorted(normalized_types)

    departments = []
    if dept_col in filtered.columns:
        dept_raws = sorted(filtered[dept_col].dropna().astype(str).str.strip().unique().tolist())
        label_col = {
            "en": "ITEM_DEPT_NAME_EN",
            "tc": "ITEM_DEPT_NAME_TC",
            "sc": "ITEM_DEPT_NAME_SC",
        }[lang]

        lab_map = {}
        if label_col in df.columns and dept_col in df.columns:
            base = df[[dept_col, label_col]].copy()
            base[dept_col] = base[dept_col].fillna("").astype(str).str.strip()
            base[label_col] = base[label_col].fillna("").astype(str).str.strip()
            base = base[base[dept_col] != ""]
            for _, r in base.iterrows():
                raw = r[dept_col]
                lbl = r[label_col] or raw
                if raw not in lab_map:
                    lab_map[raw] = lbl

        for raw in dept_raws:
            departments.append({"value": raw, "label": lab_map.get(raw) or raw})

    brands = []
    if not filtered.empty:
        brand_vals = []
        for _, r in filtered.iterrows():
            b = _brand_fallback(r, lang)
            if b:
                brand_vals.append(b)
        brands = sorted(pd.Series(brand_vals).dropna().astype(str).unique().tolist())

    item_names = []
    if q_type and q_dept and q_brand and not filtered.empty:
        disp = [_display_name_fallback(r, lang) for _, r in filtered.iterrows()]
        item_names = sorted(
            pd.Series(disp)
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )[:MAX_ITEM_NAMES]

    return jsonify({
        "types": types,
        "departments": departments,
        "brands": brands,
        "item_names": item_names,
    })


@limiter.limit(os.getenv("RATE_LIMIT_SUGGEST", "60/minute;5000/day"))
@app.get("/api/suggest")
def api_suggest():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"suggestions": []})

    try:
        df = load_df()
    except Exception:
        logger.exception("api_suggest failed")
        return jsonify({"error": "dataset unavailable"}), 500

    lang = _norm_lang(request.args.get("lang"))
    q_type = (request.args.get("type") or "").strip()
    q_dept = (request.args.get("department") or "").strip()
    q_brand = (request.args.get("brand") or "").strip()

    _, _, _, dept_col, type_col, _, _, _ = _select_cols(df, lang)

    filtered = df.copy()
    filtered = _apply_type_filter(filtered, q_type, type_col)

    if q_dept and dept_col in filtered.columns:
        filtered = filtered[filtered[dept_col].fillna("").astype(str).str.strip() == q_dept]

    if q_brand:
        filtered = filtered[_brand_mask(filtered, q_brand)]

    if filtered.empty:
        return jsonify({"suggestions": []})

    ranked = {}

    for _, row in filtered.iterrows():
        out = _display_name_fallback(row, lang).strip()
        if not out:
            continue

        score = _row_suggest_score(row, q, lang)
        if score < 0:
            continue

        out_key = _normalize_display_text(out)
        prev = ranked.get(out_key)

        if prev is None or score > prev["score"]:
            ranked[out_key] = {"score": score, "text": out}

    suggestions = [
        x["text"]
        for x in sorted(
            ranked.values(),
            key=lambda x: (-x["score"], _normalize_display_text(x["text"]))
        )
    ][:MAX_SUGGESTIONS]

    return jsonify({"suggestions": suggestions})


@limiter.limit(os.getenv("RATE_LIMIT_SEARCH", "20/minute;500/day"))
@app.post("/api/search")
def api_search():
    payload = request.get_json(silent=True) or {}
    action = (payload.get("action") or "").strip()
    lang = _norm_lang(payload.get("lang"))

    try:
        df = load_df()
    except Exception:
        logger.exception("api_search failed")
        return jsonify({"error": "dataset unavailable"}), 500

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
                product_number = _safe_str(match.iloc[0].get("Item_str")).strip()

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

        num = re.sub(r"\D", "", (payload.get("product_number") or "").strip())
        if len(num) != 8:
            error = "Please enter an 8-digit product number."
        else:
            product_number = num

    else:
        error = "Invalid action."

    if error:
        return jsonify({"error": error}), 400

    match = df[df["Item_str"] == product_number]
    if match.empty:
        return jsonify({"error": f"Product {product_number} not found."}), 404

    row = match.iloc[0]
    result = _row_to_result(row, lang)
    related = _related_items(df, row, lang)

    resp = {"result": result, "related_items": related}
    if action == "dropdown":
        resp["scroll_hint"] = {"mode": "scrollBy", "px": SCROLL_OFFSET_PX}

    return jsonify(resp)


@limiter.limit(os.getenv("RATE_LIMIT_REFRESH", "5/hour;20/day"))
@app.post("/api/refresh")
def api_refresh():
    auth = request.headers.get("Authorization", "")
    if not API_REFRESH_TOKEN or auth != f"Bearer {API_REFRESH_TOKEN}":
        return jsonify({"error": "unauthorized"}), 401

    try:
        load_df(force=True)
    except Exception:
        logger.exception("refresh failed")
        return jsonify({"error": "refresh failed"}), 500

    return jsonify({"ok": True, "message": "Data cache refreshed."})


@app.errorhandler(429)
def _ratelimit_handler(e):
    return jsonify({"error": "Too many requests. Please try again later."}), 429


@app.errorhandler(Exception)
def _unhandled(e):
    logger.exception("Unhandled error: %s", e)
    return jsonify({"error": "internal server error"}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)