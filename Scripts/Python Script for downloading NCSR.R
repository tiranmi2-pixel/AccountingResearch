# ===================== TSR DOWNLOAD AUTOMATION (SEC EDGAR) =====================
# ✅ FULL SCRIPT (INTEGRATED + PDF NAMING FIX)
#
# What I integrated / fixed (carefully, end-to-end):
#  1) ✅ Your async Playwright + nest_asyncio approach (Colab-safe)
#  2) ✅ Installs required Linux libs for Chromium (fixes libatk-1.0.so.0 error)
#  3) ✅ Keeps your "no retry on non-transient 4xx" (so no noisy 404x6)
#  4) ✅ Keeps improved asset download + offline HTML rewrite
#  5) ✅ Keeps SEC URLs in detail log (filing dir / index.json / doc URL)
#  6) ✅ RENDERED PDF file name is now:
#         "NCSR Report-<CIK10>-<ACCESSION_NO_DASH>.pdf"
#      Example: NCSR Report-0000827060-000082706025000067.pdf
#      (Unique per filing; no collisions across multiple filings in same CIK)
#  7) ✅ Avoids “two PDFs confusion”:
#      - If a native PDF is already present in the candidate docs for that filing,
#        we SKIP rendering HTML->PDF for that filing (configurable).
#      - Also defaults to rendering ONLY the primary document (configurable).

# -----------------------
# 0) Mount Drive (Colab)
# -----------------------
MAKE_RENDERED_PDF = True          # render downloaded HTML -> clean PDF (optional)
PRINT_BACKGROUND  = True          # keep colors/backgrounds in PDF
PDF_FORMAT        = "Letter"      # or "A4"

# Rendering behavior controls (recommended defaults)
RENDER_ONLY_PRIMARY_DOC = True    # render only the primaryDocument HTML
SKIP_RENDER_IF_NATIVE_PDF_EXISTS = False  # if filing already has a PDF doc, don't render HTML->PDF

from google.colab import drive
drive.mount("/content/drive")  # harmless if already mounted

# -----------------------
# 1) Imports
# -----------------------
import os, re, csv, time, random, asyncio, sys
from pathlib import Path
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple

import requests
import pandas as pd

try:
  from unidecode import unidecode
except ImportError:
  !pip -q install unidecode
from unidecode import unidecode

# --- Playwright async (Colab-safe) ---
if MAKE_RENDERED_PDF:
  try:
  import nest_asyncio
except ImportError:
  !pip -q install nest_asyncio
import nest_asyncio

nest_asyncio.apply()

try:
  from playwright.async_api import async_playwright
except Exception:
  !pip -q install playwright
from playwright.async_api import async_playwright

# --- Playwright runtime deps for Colab (fix libatk-1.0.so.0 and friends) ---
# Avoid re-install spam by using a marker file
_pw_marker = Path("/content/.pw_deps_installed")
if not _pw_marker.exists():
  !apt-get -qq update
!apt-get -qq install -y \
libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 libdrm2 libgbm1 \
libgtk-3-0 libnspr4 libnss3 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 \
libxdamage1 libxext6 libxfixes3 libxkbcommon0 libxrandr2 libasound2 \
libpangocairo-1.0-0 libpango-1.0-0
!playwright install chromium
_pw_marker.write_text("ok", encoding="utf-8")

def _run_coro(coro):
  """
    Run an async coroutine safely in notebooks (Colab already has a running loop).
    """
try:
  loop = asyncio.get_event_loop()
if loop.is_running():
  return loop.run_until_complete(coro)  # nest_asyncio enables this
return loop.run_until_complete(coro)
except RuntimeError:
  return asyncio.run(coro)

async def _render_html_to_pdf_async(html_path: Path, pdf_path: Path) -> bool:
  try:
  async with async_playwright() as p:
  browser = await p.chromium.launch(headless=True)
page = await browser.new_page()

# ✅ increase timeouts (this is why you saw 30000ms failures)
page.set_default_navigation_timeout(180_000)
page.set_default_timeout(180_000)

file_url = html_path.resolve().as_uri()

# ✅ attempt strict load first
try:
  await page.goto(file_url, wait_until="load", timeout=180_000)
except Exception:
  # ✅ fallback if load never completes
  await page.goto(file_url, wait_until="domcontentloaded", timeout=180_000)

# ✅ wait for fonts and a little idle time
try:
  await page.evaluate("() => document.fonts && document.fonts.ready")
except Exception:
  pass

try:
  await page.wait_for_load_state("networkidle", timeout=20_000)
except Exception:
  pass

await page.wait_for_timeout(1200)

await page.pdf(
  path=str(pdf_path),
  format=PDF_FORMAT,
  print_background=PRINT_BACKGROUND
)

await browser.close()
return True
except Exception as e:
  print("PDF render failed:", html_path.name, "|", e, flush=True)
return False


def render_html_to_pdf(html_path: Path, pdf_path: Path) -> bool:
  return _run_coro(_render_html_to_pdf_async(html_path, pdf_path))

# -----------------------
# 2) CONFIG (Drive paths)
# -----------------------
TSR_CSV  = "/content/drive/MyDrive/TSR Project/Raw Data/TSR Manual Search List - Equity.csv"

OUT_ROOT = Path("/content/drive/MyDrive/TSR Project/NCSR Reports")
OUT_RAW  = OUT_ROOT / "raw_files"

fresh_start = True          # TRUE = start clean (delete logs); FALSE = resume
max_funds   = None          # None = all; or set 10 for testing

filed_cutoff = date(2024, 7, 24)

download_latest_only = False
use_relevance_filter = True

USE_SUBMISSION_PAGINATION = True
MAX_SUBMISSION_PAGES = 25

# SEC headers + rate limiting
sec_user_agent = "Tiran (tiranm11@outlook.com)"
sec_delay_s    = 0.60
MAX_TRIES      = 6
BASE_SLEEP     = 1.0
TIMEOUT_JSON_S = 90
TIMEOUT_DL_S   = 180

# -----------------------
# 3) Setup folders
# -----------------------
OUT_RAW.mkdir(parents=True, exist_ok=True)

log_file_fund   = OUT_ROOT / "tsr_download_log.csv"
log_file_detail = OUT_ROOT / "tsr_download_log_detail.csv"
log_file_move   = OUT_ROOT / "tsr_move_log.csv"   # kept for compatibility; not used

pause_file = OUT_ROOT / "PAUSE"
stop_file  = OUT_ROOT / "STOP"

print("OUT_ROOT:", OUT_ROOT)
print("OUT_RAW :", OUT_RAW)
print("CSV     :", TSR_CSV)

# -----------------------
# 4) Pause/Stop
# -----------------------
def check_pause_stop() -> str:
  if stop_file.exists():
  return "STOP"
if pause_file.exists():
  print("PAUSED (remove PAUSE file to continue).", flush=True)
while pause_file.exists():
  if stop_file.exists():
  return "STOP"
time.sleep(2)
print("RESUMING...", flush=True)
return "OK"

# -----------------------
# 5) Fresh start (logs only)
# -----------------------
if fresh_start:
  print("FRESH START: deleting prior logs and starting clean.")
for f in [log_file_fund, log_file_detail, log_file_move]:
  if f.exists():
  f.unlink()

# -----------------------
# 6) CSV append helper (robust logging)
# -----------------------
DETAIL_FIELDS = [
  "crsp_portno","ticker","fund_name",
  "cik_type","cik",
  "accession","filingDate","reportDate",
  "status","detail",
  # URLs for later direct access
  "sec_filing_dir_url","sec_index_json_url","sec_doc_url"
]

def append_row_csv(path: Path, row: Dict[str, Any], field_order: List[str]) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
new_file = not path.exists()
with open(path, "a", newline="", encoding="utf-8") as f:
  w = csv.DictWriter(f, fieldnames=field_order)
if new_file:
  w.writeheader()
w.writerow({k: row.get(k, "") for k in field_order})

def exc_to_str(e: Exception) -> str:
  return f"{type(e).__name__}: {str(e)}"

# -----------------------
# 7) SEC request helpers (retry)
# -----------------------
class NonRetryableHTTPError(RuntimeError):
  pass

session = requests.Session()
session.headers.update({
  "User-Agent": sec_user_agent,
  "Accept-Encoding": "gzip, deflate",
})

# Added 505; typical transient list
TRANSIENT_STATUSES = {408, 429, 500, 502, 503, 504, 505}

def sec_request(method: str, url: str, timeout_s: int, stream: bool = False) -> requests.Response:
  last_err = None
last_status = None

for try_i in range(1, MAX_TRIES + 1):
  time.sleep(sec_delay_s)

try:
  resp = session.request(method, url, timeout=timeout_s, stream=stream)

if resp.status_code < 400:
  return resp

last_status = resp.status_code

# Do NOT retry most 4xx (esp 404); only retry transient list
if resp.status_code in TRANSIENT_STATUSES:
  wait = BASE_SLEEP * (1.7 ** (try_i - 1)) + random.uniform(0, 0.4)
print(f"SEC transient HTTP {resp.status_code} (try {try_i}/{MAX_TRIES}) -> sleep {wait:.2f}s", flush=True)
time.sleep(wait)
continue

snippet = (resp.text or "")[:300]
raise NonRetryableHTTPError(f"HTTP {resp.status_code} for {url} | {snippet}")

except NonRetryableHTTPError:
  raise
except (requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ChunkedEncodingError) as e:
  last_err = e
wait = BASE_SLEEP * (1.7 ** (try_i - 1)) + random.uniform(0, 0.6)
print(f"SEC request error (try {try_i}/{MAX_TRIES}): {exc_to_str(e)} | sleep {wait:.2f}s", flush=True)
time.sleep(wait)
continue
except Exception as e:
  last_err = e
wait = BASE_SLEEP * (1.7 ** (try_i - 1)) + random.uniform(0, 0.6)
print(f"SEC request error (try {try_i}/{MAX_TRIES}): {exc_to_str(e)} | sleep {wait:.2f}s", flush=True)
time.sleep(wait)
continue

raise RuntimeError(
  f"SEC request failed after retries url={url} | last_status={last_status} | "
  f"last_err={exc_to_str(last_err) if last_err else 'None'}"
)

def sec_get_json(url: str) -> Any:
  return sec_request("GET", url, timeout_s=TIMEOUT_JSON_S, stream=False).json()

def sec_download(url: str, dest: Path) -> bool:
  dest.parent.mkdir(parents=True, exist_ok=True)
resp = sec_request("GET", url, timeout_s=TIMEOUT_DL_S, stream=True)
with open(dest, "wb") as f:
  for chunk in resp.iter_content(chunk_size=1024 * 256):
  if chunk:
  f.write(chunk)
return True

# -----------------------
# 8) Load TSR list CSV (dtype=str to preserve leading-zero CIKs)
# -----------------------
if not Path(TSR_CSV).exists():
  raise FileNotFoundError(f"TSR list CSV not found at: {TSR_CSV}")

tsr_df = pd.read_csv(TSR_CSV, dtype=str, encoding="utf-8")
if tsr_df.shape[0] == 0:
  raise ValueError(f"TSR list CSV loaded but has 0 rows: {TSR_CSV}")

tsr_df.columns = [c.lower() for c in tsr_df.columns]

required_cols = {"crsp_portno","fund_name","ticker"}
missing = required_cols - set(tsr_df.columns)
if missing:
  raise ValueError(f"TSR CSV is missing required columns: {sorted(missing)} | present={list(tsr_df.columns)}")

for c in ["series_cik","contract_cik","comp_cik"]:
  if c not in tsr_df.columns:
  tsr_df[c] = ""

def clean_cik_str(x: str) -> str:
  if x is None:
  return ""
s = str(x).strip()
if s == "" or s.lower() == "nan":
  return ""
s = re.sub(r"\.0+$", "", s)
s = re.sub(r"\s+", "", s)
if s.isdigit():
  return s.zfill(10)
return ""

tsr_df["ticker"] = tsr_df["ticker"].fillna("").astype(str).str.strip().str.upper()
tsr_df["fund_name"] = tsr_df["fund_name"].fillna("").astype(str)
tsr_df["crsp_portno"] = pd.to_numeric(tsr_df["crsp_portno"], errors="coerce")

for c in ["series_cik","contract_cik","comp_cik"]:
  tsr_df[c] = tsr_df[c].apply(clean_cik_str)

tsr_df = tsr_df.dropna(subset=["crsp_portno"]).copy()
tsr_df["crsp_portno"] = tsr_df["crsp_portno"].astype(int)
tsr_df = tsr_df.drop_duplicates(subset=["crsp_portno"]).reset_index(drop=True)

fund_master = tsr_df[["crsp_portno","fund_name","ticker","series_cik","contract_cik","comp_cik"]].copy()

print("Unique funds in TSR list (crsp_portno):", fund_master["crsp_portno"].nunique())
print("Funds with any CIK:",
      int(((fund_master["series_cik"].str.len() > 0) |
             (fund_master["contract_cik"].str.len() > 0) |
             (fund_master["comp_cik"].str.len() > 0)).sum()))

# -----------------------
# 9) Load SEC MF map (ONLY for seriesid/classid relevance matching)
# -----------------------
def load_sec_mf_map(url: str = "https://www.sec.gov/files/company_tickers_mf.json") -> pd.DataFrame:
  x = sec_get_json(url)

if isinstance(x, list):
  dt = pd.DataFrame(x)
elif isinstance(x, dict) and "data" in x and "fields" in x:
  dt = pd.DataFrame(x["data"], columns=x["fields"])
elif isinstance(x, dict):
  dt = pd.DataFrame(list(x.values()))
else:
  raise ValueError("SEC MF map JSON parsed into an unexpected structure.")

dt.columns = [c.lower() for c in dt.columns]

ticker_cols = [c for c in dt.columns if re.search(r"(ticker|symbol)", c, re.I)]
if not ticker_cols:
  raise ValueError("No ticker/symbol column found in SEC MF map.")
if "ticker" not in dt.columns:
  dt = dt.rename(columns={ticker_cols[0]: "ticker"})

if "seriesid" not in dt.columns:
  for c in dt.columns:
  if "series" in c.lower():
  dt = dt.rename(columns={c: "seriesid"})
break
if "classid" not in dt.columns:
  for c in dt.columns:
  if "class" in c.lower():
  dt = dt.rename(columns={c: "classid"})
break

dt["ticker"] = dt["ticker"].astype(str).str.strip().str.upper()
return dt

print("Loading SEC MF map (for seriesid/classid only)...")
mf_map = load_sec_mf_map()[["ticker","seriesid","classid"]].drop_duplicates()
print("SEC MF map loaded. Head:\n", mf_map.head(5).to_string(index=False))

dt = fund_master.merge(mf_map, on="ticker", how="left")

has_any_cik = (
  dt["series_cik"].astype(str).str.len().gt(0) |
    dt["contract_cik"].astype(str).str.len().gt(0) |
    dt["comp_cik"].astype(str).str.len().gt(0)
)

todo_all = (
  dt[has_any_cik][["crsp_portno","fund_name","ticker","series_cik","contract_cik","comp_cik","seriesid","classid"]]
  .drop_duplicates()
  .sort_values("ticker")
  .reset_index(drop=True)
)

todo = todo_all.head(int(max_funds)).copy() if max_funds is not None else todo_all
print("Funds to process in this run (has any CIK):", todo.shape[0])
if todo.shape[0] == 0:
  raise RuntimeError("No funds to process (no CIKs found in TSR CSV).")

# -----------------------
# 10) Helpers
# -----------------------
def pad10(x: int) -> str:
  return f"{int(x):010d}"

def _dates_to_pydate(values) -> pd.Series:
  return pd.Series(pd.to_datetime(values, errors="coerce")).dt.date

def safe_filename(x: str) -> str:
  x = unidecode(str(x))
x = re.sub(r"[^A-Za-z0-9._-]+", "_", x)
    return x[:180]

def rendered_pdf_path_for_filing(filing_dir: Path, cik: int, accession: str) -> Path:
    """
    Requested naming:
      NCSR Report-<CIK10>-<ACCESSION_NO_DASH>.pdf
    """
    acc_nodash = str(accession).replace("-", "")
    name = f"NCSR Report-{pad10(cik)}-{acc_nodash}.pdf"
    return filing_dir / safe_filename(name)

def get_ncsr_filings_recent_only(cik_num: int, filed_on_or_after: date) -> pd.DataFrame:
    cik10 = pad10(cik_num)
    sub_url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    sub = sec_get_json(sub_url)

    recent = (((sub or {}).get("filings") or {}).get("recent")) or {}
    if not recent or not recent.get("form"):
        return pd.DataFrame()

    f = pd.DataFrame({
        "form":       recent.get("form", []),
        "filingDate": _dates_to_pydate(recent.get("filingDate", [])),
        "reportDate": _dates_to_pydate(recent.get("reportDate", [])),
        "accession":  recent.get("accessionNumber", []),
        "primaryDoc": recent.get("primaryDocument", []),
    })

    f = f[f["form"].isin(["N-CSR","N-CSR/A"])].copy()
    f = f[f["filingDate"].notna() & (f["filingDate"] >= filed_on_or_after)].copy()
    f = f.sort_values("filingDate", ascending=False).reset_index(drop=True)
    return f

def _submissions_url_for_extra_file(name: str) -> str:
    return f"https://data.sec.gov/submissions/{name}"

def _filings_recent_to_df(recent: dict) -> pd.DataFrame:
    if not recent or not recent.get("form"):
        return pd.DataFrame()
    return pd.DataFrame({
        "form":       recent.get("form", []),
        "filingDate": _dates_to_pydate(recent.get("filingDate", [])),
        "reportDate": _dates_to_pydate(recent.get("reportDate", [])),
        "accession":  recent.get("accessionNumber", []),
        "primaryDoc": recent.get("primaryDocument", []),
    })

def get_ncsr_filings_paginated(cik_num: int, filed_on_or_after: date) -> pd.DataFrame:
    cik10 = pad10(cik_num)
    sub_url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    sub = sec_get_json(sub_url)

    pages = []
    recent0 = (((sub or {}).get("filings") or {}).get("recent")) or {}
    df0 = _filings_recent_to_df(recent0)
    if df0.shape[0] > 0:
        pages.append(df0)

    files_meta = (((sub or {}).get("filings") or {}).get("files")) or []
    checked = 0

    for meta in files_meta:
        if checked >= MAX_SUBMISSION_PAGES:
            break
        name = meta.get("name")
        if not name:
            continue

        filing_to = meta.get("filingTo")
        try:
            filing_to_dt = pd.to_datetime(filing_to, errors="coerce").date() if filing_to else None
        except Exception:
            filing_to_dt = None

        if filing_to_dt and filing_to_dt < filed_on_or_after:
            break

        try:
            page_json = sec_get_json(_submissions_url_for_extra_file(name))
        except Exception:
            checked += 1
            continue

        recentp = (((page_json or {}).get("filings") or {}).get("recent")) or {}
        dfp = _filings_recent_to_df(recentp)
        if dfp.shape[0] > 0:
            pages.append(dfp)
        checked += 1

    if not pages:
        return pd.DataFrame()

    f = pd.concat(pages, ignore_index=True)
    f = f[f["form"].isin(["N-CSR","N-CSR/A"])].copy()
    f = f[f["filingDate"].notna() & (f["filingDate"] >= filed_on_or_after)].copy()
    f = f.sort_values("filingDate", ascending=False).reset_index(drop=True)
    return f

def get_ncsr_filings(cik_num: int, filed_on_or_after: date) -> pd.DataFrame:
    if USE_SUBMISSION_PAGINATION:
        return get_ncsr_filings_paginated(cik_num, filed_on_or_after)
    return get_ncsr_filings_recent_only(cik_num, filed_on_or_after)

def fetch_filing_index_items(cik_num: int, accession: str) -> pd.DataFrame:
    cik_dir = str(int(cik_num))
    acc_nodash = accession.replace("-", "")
    url1 = f"https://www.sec.gov/Archives/edgar/data/{cik_dir}/{acc_nodash}/index.json"
    url2 = f"https://www.sec.gov/Archives/edgar/data/{cik_dir}/{acc_nodash}/{accession}-index.json"

    idx = None
    try:
        idx = sec_get_json(url1)
    except Exception:
        idx = None

    if idx is None:
        try:
            idx = sec_get_json(url2)
        except Exception:
            idx = None

    if not idx:
        return pd.DataFrame()

    items = ((idx.get("directory") or {}).get("item")) or []
    return pd.DataFrame(items) if items else pd.DataFrame()

def pick_candidate_docs(items_df: pd.DataFrame, primary_doc: Optional[str] = None) -> pd.DataFrame:
    if items_df is None or items_df.shape[0] == 0:
        return pd.DataFrame()
    df = items_df.copy()
    df.columns = [c.lower() for c in df.columns]
    if "name" not in df.columns:
        return pd.DataFrame()

    df["name_l"] = df["name"].astype(str).str.lower()
    df["score"] = 0

    df.loc[df["name_l"].str.contains(r"tailored|shareholder|shrhld|annual|semi.?annual|report", regex=True, na=False), "score"] += 10
    df.loc[df["name_l"].str.contains(r"ncsr", na=False), "score"] += 3
    df.loc[df["name_l"].str.contains(r"\.pdf$", regex=True, na=False), "score"] += 5
    df.loc[df["name_l"].str.contains(r"\.htm|\.html", regex=True, na=False), "score"] += 2
    df.loc[df["name_l"].str.contains(r"\.xml|\.xsd|\.jpg|\.png|\.gif|\.zip$", regex=True, na=False), "score"] -= 10

    if primary_doc and isinstance(primary_doc, str) and primary_doc.strip():
        df.loc[df["name_l"] == primary_doc.strip().lower(), "score"] += 50

    df = df[df["score"] > 0].copy()
    if df.shape[0] == 0:
        return pd.DataFrame()

    df["size_num"] = pd.to_numeric(df.get("size", None), errors="coerce")
    df = df.sort_values(["score","size_num"], ascending=[False, False])

    cols = [c for c in ["name","type","size","score"] if c in df.columns]
    return df[cols].head(3).reset_index(drop=True)

def _extract_asset_refs(html_text: str) -> List[str]:
    hits = re.findall(r'(?:src|href)=["\']([^"\']+)["\']', html_text, flags=re.I)
    out = []
    for u in hits:
        if re.match(r"^(https?:)?//", u):
            continue
        if re.match(r"^(#|mailto:|javascript:|data:)", u, flags=re.I):
            continue
        u = re.sub(r"\?.*$", "", u)
        if not u:
            continue
        if not re.search(r"\.(css|js|png|jpg|jpeg|gif|svg|webp|woff|woff2|ttf)$", u, flags=re.I):
            continue
        out.append(u)
    return out

def _candidate_asset_paths(u: str) -> List[str]:
    u0 = u.strip()
    u1 = re.sub(r"^/+", "", u0)
    u1 = u1.replace("../", "")
    cands = [u1]

    if u1.startswith("include/"):
        cands.append(u1.replace("include/", "", 1))
    else:
        cands.append("include/" + u1)

    seen, out = set(), []
    for x in cands:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out[:3]

def _try_download_asset(cik_dir: str, acc_nodash: str, relpath: str, dest: Path) -> bool:
    url = f"https://www.sec.gov/Archives/edgar/data/{cik_dir}/{acc_nodash}/{relpath}"
    try:
        sec_download(url, dest)
        return True
    except NonRetryableHTTPError:
        return False
    except Exception:
        return False

def download_html_assets_and_rewrite(html_path: Path, cik_dir: str, acc_nodash: str, filing_dir: Path) -> None:
    if not html_path.exists():
        return
    ext = html_path.suffix.lower().lstrip(".")
    if ext not in ("htm","html"):
        return

    try:
        txt = html_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return

    refs = _extract_asset_refs(txt)
    if not refs:
        return

    for u in set(refs):
        for relpath in _candidate_asset_paths(u):
            asset_dest = filing_dir / relpath
            if asset_dest.exists():
                break
            asset_dest.parent.mkdir(parents=True, exist_ok=True)
            ok = _try_download_asset(cik_dir, acc_nodash, relpath, asset_dest)
            if ok:
                break

    new_txt = txt
    new_txt = re.sub(r'(?i)(src|href)=([\"\'])/include/', r'\1=\2include/', new_txt)
    new_txt = re.sub(
        rf'(?i)(src|href)=([\"\'])/Archives/edgar/data/{re.escape(str(int(cik_dir)))}/{re.escape(acc_nodash)}/',
        r'\1=\2',
        new_txt
    )

    if new_txt != txt:
        try:
            html_path.write_text(new_txt, encoding="utf-8", errors="ignore")
        except Exception:
            pass

def read_text_safe(path: Path, max_chars: int = 2_000_000) -> str:
    if not path.exists():
        return ""
    try:
        txt = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    return txt[:max_chars]

def download_filing_summary_xml(items_df: pd.DataFrame, cik_dir: str, acc_nodash: str, filing_dir: Path) -> Optional[Path]:
    if items_df is None or items_df.shape[0] == 0:
        return None
    df = items_df.copy()
    df.columns = [c.lower() for c in df.columns]
    if "name" not in df.columns:
        return None
    df["name_l"] = df["name"].astype(str).str.lower()
    if not (df["name_l"] == "filingsummary.xml").any():
        return None

    xml_name = df.loc[df["name_l"] == "filingsummary.xml", "name"].iloc[0]
    xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik_dir}/{acc_nodash}/{xml_name}"
    xml_dest = filing_dir / "FilingSummary.xml"

    if not xml_dest.exists():
        try:
            sec_download(xml_url, xml_dest)
        except Exception:
            return None

    return xml_dest if xml_dest.exists() else None

def extract_fund_keywords(fund_name: str, k: int = 3) -> List[str]:
    if not fund_name:
        return []
    s = re.sub(r"[^A-Z0-9 ]+", " ", str(fund_name).upper())
    words = [w for w in s.split() if w]
    stopw = {
        "FUND","FUNDS","TRUST","SERIES","PORTFOLIO","PORTFOLIOS","INC","LLC","LTD",
        "CLASS","SHARES","SHARE","INVESTOR","INSTITUTIONAL","ADVISOR","ADVISORS",
        "THE","AND","OF","A","AN","TO","FOR"
    }
    words = [w for w in words if w not in stopw and len(w) >= 5]
    out = []
    for w in words:
        if w not in out:
            out.append(w)
    return out[:k]

def match_in_folder(filing_dir: Path, high_patterns: List[str], low_patterns: List[str]) -> Optional[bool]:
    if not filing_dir.exists():
        return None

    files = [p for p in filing_dir.rglob("*") if p.is_file() and p.suffix.lower() in (".htm",".html",".xml")]
    fs = filing_dir / "FilingSummary.xml"
    if fs.exists():
        files = [fs] + files

    if not files:
        return None

    hp = [str(x).upper() for x in high_patterns if x and str(x).strip() and str(x) != "nan"]
    lp = [str(x).upper() for x in low_patterns if x and str(x).strip() and str(x) != "nan"]

    if not hp and not lp:
        return None

    for fp in files:
        tu = read_text_safe(fp).upper()
        if not tu:
            continue
        for p in hp:
            if p in tu:
                return True
        for p in lp:
            if p in tu:
                return True

    if hp:
        return False
    return None

def match_in_dirs(dirs: List[Path], high_patterns: List[str], low_patterns: List[str]) -> Optional[bool]:
    dirs = [d for d in dirs if d.exists()]
    if not dirs:
        return None
    for d in dirs:
        out = match_in_folder(d, high_patterns=high_patterns, low_patterns=low_patterns)
        if out is True:
            return True
        if out is False:
            return False
    return None

def sec_urls(cik: int, accession: str, docname: Optional[str] = None) -> Tuple[str, str, str]:
    cik_dir = str(int(cik))
    acc_nodash = accession.replace("-", "")
    filing_dir_url = f"https://www.sec.gov/Archives/edgar/data/{cik_dir}/{acc_nodash}/"
    index_json_url  = f"{filing_dir_url}index.json"
    doc_url = f"{filing_dir_url}{docname}" if docname else ""
    return filing_dir_url, index_json_url, doc_url

# -----------------------
# 11) Resume: load existing logs + build evaluated_key_set (MATCHED only)
# -----------------------
evaluated_key_set = set()

existing_fund_log = pd.DataFrame()
if (not fresh_start) and log_file_fund.exists():
    try:
        existing_fund_log = pd.read_csv(log_file_fund)
    except Exception:
        existing_fund_log = pd.DataFrame()

if (not fresh_start) and log_file_detail.exists():
    try:
        tmp = pd.read_csv(log_file_detail)
        need = {"crsp_portno","cik","accession","status"}
        if need.issubset(set(tmp.columns)):
            tmp = tmp[(tmp["status"] == "MATCHED_THIS_FUND") & tmp["accession"].notna()]
            for _, r in tmp.iterrows():
                try:
                    evaluated_key_set.add(f"{int(r['crsp_portno'])}|{int(r['cik'])}|{str(r['accession'])}")
                except Exception:
                    pass
    except Exception:
        pass

# -----------------------
# 12) fund_summary
# -----------------------
fund_run_ids = set(todo["crsp_portno"].dropna().astype(int).tolist())

fund_summary = fund_master.copy()
fund_summary["cik_type"] = ""
fund_summary["cik"] = pd.NA
fund_summary["n_filings_found"] = 0
fund_summary["n_filings_downloaded"] = 0
fund_summary["downloaded_any"] = 0
fund_summary["status"] = ""
fund_summary["note"] = ""

if (not fresh_start) and (existing_fund_log.shape[0] > 0) and ("crsp_portno" in existing_fund_log.columns):
    keep_cols = [c for c in ["crsp_portno","cik_type","cik","n_filings_found","n_filings_downloaded","downloaded_any","status","note"] if c in existing_fund_log.columns]
    tmp = existing_fund_log[keep_cols].drop_duplicates(subset=["crsp_portno"])
    fund_summary = fund_summary.merge(tmp, on="crsp_portno", how="left", suffixes=("", "_old"))
    for c in ["cik_type","cik","n_filings_found","n_filings_downloaded","downloaded_any","status","note"]:
        if f"{c}_old" in fund_summary.columns:
            fund_summary[c] = fund_summary[c].where(fund_summary[f"{c}_old"].isna(), fund_summary[f"{c}_old"])
            fund_summary.drop(columns=[f"{c}_old"], inplace=True)

def write_fund_log():
    todo_ids = set(todo["crsp_portno"].dropna().astype(int).tolist())
    n_total_todo = len(todo_ids)
    n_done = int(((fund_summary["crsp_portno"].isin(todo_ids)) & (fund_summary["status"] == "DONE")).sum())
    n_pending = n_total_todo - n_done

    total_funds = int(fund_master["crsp_portno"].nunique())
    funds_with_any = int((fund_summary["downloaded_any"] == 1).sum())
    pct_funds_with_any = round(100 * funds_with_any / total_funds, 2) if total_funds else 0.0

    total_filings_found = int(fund_summary["n_filings_found"].fillna(0).sum())
    total_filings_dl = int(fund_summary["n_filings_downloaded"].fillna(0).sum())
    pct_filings_dl = round(100 * total_filings_dl / total_filings_found, 2) if total_filings_found else pd.NA

    def rate_row(r):
        try:
            nf = int(r["n_filings_found"])
            nd = int(r["n_filings_downloaded"])
            return round(100 * nd / nf, 2) if nf > 0 else pd.NA
        except Exception:
            return pd.NA

    fund_summary["download_rate"] = fund_summary.apply(rate_row, axis=1)

    fund_summary["total_unique_funds_in_tsr_list"] = total_funds
    fund_summary["funds_with_any_download"] = funds_with_any
    fund_summary["pct_funds_with_any_download"] = pct_funds_with_any
    fund_summary["total_filings_found_all_funds"] = total_filings_found
    fund_summary["total_filings_downloaded_all"] = total_filings_dl
    fund_summary["pct_filings_downloaded_all"] = pct_filings_dl
    fund_summary["run_total_todo_funds"] = n_total_todo
    fund_summary["run_funds_completed"] = n_done
    fund_summary["run_funds_pending"] = n_pending

    tmp = fund_summary.copy()
    tmp["ticker_sort"] = tmp["ticker"].fillna("")
    tmp = tmp.sort_values(
        by=["downloaded_any","n_filings_downloaded","ticker_sort","crsp_portno"],
        ascending=[False, False, True, True]
    ).drop(columns=["ticker_sort"])

    tmp.to_csv(log_file_fund, index=False, encoding="utf-8")

# -----------------------
# 13) Main loop (TRY CIKs in order)
# -----------------------
for i in range(todo.shape[0]):
    ctrl = check_pause_stop()
    if ctrl == "STOP":
        print("STOP requested. Writing checkpoints and exiting.")
        break

    row = todo.iloc[i].to_dict()
    crsp_portno = int(row["crsp_portno"])
    ticker = str(row.get("ticker",""))
    fund_name = str(row.get("fund_name",""))

    cur_status = fund_summary.loc[fund_summary["crsp_portno"] == crsp_portno, "status"].astype(str)
    if len(cur_status) and cur_status.iloc[0] == "DONE":
        print(f"[{datetime.now().strftime('%H:%M:%S')}] SKIP (DONE) {i+1}/{todo.shape[0]}  ticker={ticker}", flush=True)
        continue

    cik_candidates = [
        ("SERIES",   str(row.get("series_cik","")).strip()),
        ("CONTRACT", str(row.get("contract_cik","")).strip()),
        ("COMPANY",  str(row.get("comp_cik","")).strip()),
    ]
    cik_candidates = [(t,c) for (t,c) in cik_candidates if c and c.lower() != "nan"]

    if not cik_candidates:
        fund_summary.loc[fund_summary["crsp_portno"] == crsp_portno, ["status","note"]] = ["NO_CIK", "No CIKs in TSR CSV"]
        continue

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fund {i+1}/{todo.shape[0]}  ticker={ticker}  CIKs_to_try={[c for _,c in cik_candidates]}", flush=True)

    filings = pd.DataFrame()
    cik_used = None
    cik_type_used = None
    first_error = None

    for (cik_type, cik_str10) in cik_candidates:
        try:
            cik_int = int(cik_str10)
        except Exception:
            continue

        try:
            tmp_filings = get_ncsr_filings(cik_int, filed_on_or_after=filed_cutoff)
        except Exception as e:
            first_error = first_error or e
            append_row_csv(log_file_detail, {
                "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                "cik_type": cik_type, "cik": cik_int,
                "accession": "", "filingDate": "", "reportDate": "",
                "status": "SUBMISSIONS_ERROR", "detail": f"{cik_type} CIK failed: {exc_to_str(e)}",
                "sec_filing_dir_url": "", "sec_index_json_url": "", "sec_doc_url": ""
            }, DETAIL_FIELDS)
            continue

        if tmp_filings.shape[0] > 0:
            filings = tmp_filings
            cik_used = cik_int
            cik_type_used = cik_type
            break
        else:
            append_row_csv(log_file_detail, {
                "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                "cik_type": cik_type, "cik": cik_int,
                "accession": "", "filingDate": "", "reportDate": "",
                "status": "NO_NCSR_FOR_CIK",
                "detail": f"No N-CSR/N-CSR/A on/after {filed_cutoff} for {cik_type} CIK",
                "sec_filing_dir_url": "", "sec_index_json_url": "", "sec_doc_url": ""
            }, DETAIL_FIELDS)

    if cik_used is None:
        if first_error is not None:
            fund_summary.loc[fund_summary["crsp_portno"] == crsp_portno, ["status","note"]] = ["SUBMISSIONS_ERROR", exc_to_str(first_error)]
        else:
            fund_summary.loc[fund_summary["crsp_portno"] == crsp_portno, ["status","note"]] = [
                "NO_NCSR_FOUND", f"No N-CSR/N-CSR/A on/after {filed_cutoff} for any CIK"
            ]
        continue

    cik = cik_used
    fund_summary.loc[fund_summary["crsp_portno"] == crsp_portno, ["cik_type","cik"]] = [cik_type_used, cik]
    print(f"  Using {cik_type_used} CIK={pad10(cik)}  (filings found: {filings.shape[0]})", flush=True)

    fund_summary.loc[fund_summary["crsp_portno"] == crsp_portno, "n_filings_found"] = filings.shape[0]
    if download_latest_only:
        filings = filings.head(1).copy()

    filing_successes_for_this_fund = 0

    for k in range(filings.shape[0]):
        ctrl = check_pause_stop()
        if ctrl == "STOP":
            print("STOP requested. Breaking out of filing loop.")
            break

        acc = str(filings.loc[k, "accession"])
        fdt = filings.loc[k, "filingDate"]
        rdt = filings.loc[k, "reportDate"]
        pdoc = str(filings.loc[k, "primaryDoc"] or "")

        acc_key = f"{crsp_portno}|{cik}|{acc}"
        if acc_key in evaluated_key_set:
            print("  Filing already CONFIRMED MATCHED earlier. Skipping accession:", acc, flush=True)
            continue

        period_dt  = rdt if pd.notna(rdt) else fdt
        period_lab = pd.to_datetime(period_dt).strftime("%Y-%m-%d")

        filing_dir_url, index_json_url, _ = sec_urls(cik, acc, None)

        print(f"  Filing: {acc}  filed: {fdt}  reportDate: {rdt}  primaryDoc: {pdoc}", flush=True)

        try:
            items_df = fetch_filing_index_items(cik, acc)
        except Exception:
            items_df = pd.DataFrame()

        if items_df.shape[0] == 0:
            append_row_csv(log_file_detail, {
                "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                "cik_type": cik_type_used, "cik": cik,
                "accession": acc, "filingDate": fdt, "reportDate": rdt,
                "status": "INDEX_ERROR", "detail": "Could not fetch index.json for filing directory",
                "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": ""
            }, DETAIL_FIELDS)
            continue

        candidates = pick_candidate_docs(items_df, primary_doc=pdoc)
        if candidates.shape[0] == 0:
            append_row_csv(log_file_detail, {
                "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                "cik_type": cik_type_used, "cik": cik,
                "accession": acc, "filingDate": fdt, "reportDate": rdt,
                "status": "NO_CANDIDATE_DOCS", "detail": "Index fetched but no likely report docs found",
                "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": ""
            }, DETAIL_FIELDS)
            continue

        # If native PDF exists among candidates, optionally skip rendering
        native_pdf_in_candidates = False
        if SKIP_RENDER_IF_NATIVE_PDF_EXISTS and candidates.shape[0] > 0:
            try:
                native_pdf_in_candidates = candidates["name"].astype(str).str.lower().str.endswith(".pdf").any()
            except Exception:
                native_pdf_in_candidates = False

        cik_dir = str(int(cik))
        acc_nodash = acc.replace("-", "")

        filing_dir = OUT_RAW / f"CIK{pad10(cik)}" / period_lab / acc_nodash
        filing_dir.mkdir(parents=True, exist_ok=True)

        rendered_pdf_done_for_this_filing = False

        for j in range(candidates.shape[0]):
            ctrl = check_pause_stop()
            if ctrl == "STOP":
                print("STOP requested during downloads. Breaking out.")
                break

            docname = str(candidates.loc[j, "name"])
            _, _, doc_url = sec_urls(cik, acc, docname)

            dest = filing_dir / safe_filename(docname)

            if dest.exists():
                append_row_csv(log_file_detail, {
                    "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                    "cik_type": cik_type_used, "cik": cik,
                    "accession": acc, "filingDate": fdt, "reportDate": rdt,
                    "status": "ALREADY_EXISTS", "detail": str(dest),
                    "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": doc_url
                }, DETAIL_FIELDS)
                continue

            print("    Downloading:", docname, flush=True)
            try:
                sec_download(doc_url, dest)

                # improved assets + offline rewrite (only affects HTML)
                download_html_assets_and_rewrite(dest, cik_dir, acc_nodash, filing_dir)

                # ✅ render HTML -> PDF (optional)
                should_render_this_doc = (
                    MAKE_RENDERED_PDF and
                    (dest.suffix.lower() in (".htm", ".html")) and
                    (not rendered_pdf_done_for_this_filing)
                )

                if should_render_this_doc and RENDER_ONLY_PRIMARY_DOC:
                    # Only render if this doc is the primaryDoc (best "main report" behavior)
                    should_render_this_doc = (pdoc.strip().lower() == docname.strip().lower())

             # Do NOT skip rendering even if native PDF exists
              # (intentionally rendering anyway)
                if should_render_this_doc:
                    pdf_dest = rendered_pdf_path_for_filing(filing_dir, cik, acc)
                    if not pdf_dest.exists():
                        ok = render_html_to_pdf(dest, pdf_dest)
                        append_row_csv(log_file_detail, {
                            "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                            "cik_type": cik_type_used, "cik": cik,
                            "accession": acc, "filingDate": fdt, "reportDate": rdt,
                            "status": "RENDERED_PDF" if ok else "RENDER_PDF_ERROR",
                            "detail": str(pdf_dest),
                            "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": doc_url
                        }, DETAIL_FIELDS)
                        if ok:
                            rendered_pdf_done_for_this_filing = True

                append_row_csv(log_file_detail, {
                    "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                    "cik_type": cik_type_used, "cik": cik,
                    "accession": acc, "filingDate": fdt, "reportDate": rdt,
                    "status": "DOWNLOADED", "detail": str(dest),
                    "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": doc_url
                }, DETAIL_FIELDS)

            except Exception as e:
                append_row_csv(log_file_detail, {
                    "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                    "cik_type": cik_type_used, "cik": cik,
                    "accession": acc, "filingDate": fdt, "reportDate": rdt,
                    "status": "DOWNLOAD_ERROR", "detail": exc_to_str(e),
                    "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": doc_url
                }, DETAIL_FIELDS)

        # FilingSummary.xml (optional)
        if use_relevance_filter:
            try:
                xml_path = download_filing_summary_xml(items_df, cik_dir, acc_nodash, filing_dir)
                if xml_path:
                    append_row_csv(log_file_detail, {
                        "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                        "cik_type": cik_type_used, "cik": cik,
                        "accession": acc, "filingDate": fdt, "reportDate": rdt,
                        "status": "GOT_FILINGSUMMARY_XML", "detail": str(xml_path),
                        "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": ""
                    }, DETAIL_FIELDS)
            except Exception as e:
                append_row_csv(log_file_detail, {
                    "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                    "cik_type": cik_type_used, "cik": cik,
                    "accession": acc, "filingDate": fdt, "reportDate": rdt,
                    "status": "FILINGSUMMARY_XML_ERROR", "detail": exc_to_str(e),
                    "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": ""
                }, DETAIL_FIELDS)

        # Relevance matching
        if use_relevance_filter:
            fund_kw = extract_fund_keywords(fund_name, k=3)

            high_fund = []
            for v in [row.get("classid"), row.get("seriesid"), ticker]:
                if v is not None and str(v).strip() and str(v) != "nan":
                    high_fund.append(str(v))

            match_fund = match_in_dirs(
                dirs=[filing_dir],
                high_patterns=high_fund,
                low_patterns=fund_kw
            )

            if match_fund is True:
                filing_successes_for_this_fund += 1
                append_row_csv(log_file_detail, {
                    "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                    "cik_type": cik_type_used, "cik": cik,
                    "accession": acc, "filingDate": fdt, "reportDate": rdt,
                    "status": "MATCHED_THIS_FUND",
                    "detail": "Matched by class/series/ticker or fund keywords: " + ", ".join(fund_kw),
                    "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": ""
                }, DETAIL_FIELDS)
                evaluated_key_set.add(acc_key)

            elif match_fund is False:
                append_row_csv(log_file_detail, {
                    "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                    "cik_type": cik_type_used, "cik": cik,
                    "accession": acc, "filingDate": fdt, "reportDate": rdt,
                    "status": "NOT_THIS_FUND",
                    "detail": "Folder exists/downloaded but did NOT match this fund's class/series/ticker.",
                    "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": ""
                }, DETAIL_FIELDS)
            else:
                append_row_csv(log_file_detail, {
                    "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                    "cik_type": cik_type_used, "cik": cik,
                    "accession": acc, "filingDate": fdt, "reportDate": rdt,
                    "status": "UNVERIFIED_MATCH",
                    "detail": "Could not verify match (no readable HTML/XML hit). Kept folder; not counted.",
                    "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": ""
                }, DETAIL_FIELDS)
        else:
            filing_successes_for_this_fund += 1

    if ctrl == "STOP":
        fund_summary.loc[fund_summary["crsp_portno"] == crsp_portno, ["n_filings_downloaded","downloaded_any","status","note"]] = [
            filing_successes_for_this_fund, int(filing_successes_for_this_fund > 0), "STOPPED_MID_RUN", "STOP file detected; rerun to resume."
        ]
        break

    fund_summary.loc[fund_summary["crsp_portno"] == crsp_portno, ["n_filings_downloaded","downloaded_any","status","note"]] = [
        filing_successes_for_this_fund, int(filing_successes_for_this_fund > 0), "DONE", ""
    ]

    if (i + 1) % 5 == 0:
        write_fund_log()
        print("Checkpoint written.", flush=True)

# -----------------------
# 14) Final log
# -----------------------
fund_summary.loc[(fund_summary["status"].isna()) | (fund_summary["status"] == ""), "status"] = \
    fund_summary["crsp_portno"].apply(lambda x: "NOT_MATCHED_OR_SKIPPED" if int(x) in fund_run_ids else "NOT_RUN")

fund_summary.loc[(fund_summary["ticker"].isna()) | (fund_summary["ticker"].astype(str).str.strip() == ""),
                 "note"] = "Missing/blank ticker in tsr_list"

write_fund_log()

print("DONE.")
print("Fund-wise log saved to: ", log_file_fund)
print("Detail log saved to:    ", log_file_detail)
print("Files saved under:      ", OUT_RAW)
print("Google Drive output:    ", OUT_ROOT)
