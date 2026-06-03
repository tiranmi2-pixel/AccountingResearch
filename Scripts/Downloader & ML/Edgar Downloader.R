# ============================================================
# SEC EDGAR N-CSR downloader for the TSR project
#
# Note: This is Python code, not R code.
# It is uploaded to GitHub only for information and transparency about the EDGAR/TSR data process.
#
# What this script does:
#   - Reads the TSR manual search list created by the R pipeline.
#   - Downloads N-CSR and N-CSR/A filings from SEC EDGAR.
#   - Saves filing HTML/PDF/XML files into local folders.
#   - Keeps detailed logs so the download can resume safely.
#   - Optionally renders downloaded HTML filings into PDF files.
#   - Uses pause/stop files so a long run can be controlled without editing code.
# ============================================================

# -----------------------
# 0) Rendering and resume settings
# -----------------------
# Turn this on when you want the script to render downloaded HTML filings as PDF.
MAKE_RENDERED_PDF = True

# Keep filing colors and backgrounds when rendering HTML into PDF.
PRINT_BACKGROUND  = True

# PDF page format used by Playwright.
PDF_FORMAT        = "Letter"      # or "A4"

# Render only the SEC primaryDocument HTML rather than every downloaded HTML candidate.
RENDER_ONLY_PRIMARY_DOC = True

# If a filing already includes a native PDF, this option can skip HTML-to-PDF rendering.
SKIP_RENDER_IF_NATIVE_PDF_EXISTS = False

# Controls the filing folder structure used for saved downloads.
USE_STABLE_FILING_DIR = False

# Avoid downloading the same filing again when the folder already has usable files.
SKIP_DOWNLOAD_IF_FOLDER_HAS_FILES = True

# Write a small completion marker after a filing folder has been successfully downloaded.
WRITE_DOWNLOAD_MARKER = True

# Keep this False when you want completed funds to be skipped on reruns.
FORCE_RECHECK_DONE_FUNDS = False

# -----------------------
# 1) Imports
# -----------------------
import os, re, csv, time, random, asyncio
from pathlib import Path
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple

import requests
import pandas as pd

from unidecode import unidecode

# Playwright is only needed if HTML-to-PDF rendering is enabled.
async_playwright = None
nest_asyncio = None

if MAKE_RENDERED_PDF:
  import nest_asyncio
nest_asyncio.apply()
from playwright.async_api import async_playwright

# -----------------------
# 2) File paths and run settings
# -----------------------
# Input CSV created by the R script: 05_filter_list_for_downloader.R
TSR_CSV  = "/root/tsr_project/TSR Manual Search List - Equity.csv"

# Main output folders for downloaded filings and logs.
OUT_ROOT = Path("/root/tsr_project/NCSR Reports")
OUT_RAW  = OUT_ROOT / "raw_files"

# Set this to True only when you want to delete old logs and restart cleanly.
fresh_start = False

# Use None to process all funds. Set a small number such as 10 for testing.
max_funds   = None

# Only filings on or after this date will be considered.
filed_cutoff = date(2023, 1, 1)

# If True, only the latest N-CSR/N-CSR/A filing is downloaded for each fund.
download_latest_only = False

# If True, the script checks whether the downloaded filing appears to match the target fund.
use_relevance_filter = True

# Use older SEC submission pages in addition to the recent filings list.
USE_SUBMISSION_PAGINATION = True
MAX_SUBMISSION_PAGES = 25

# SEC request settings. The user-agent should identify the requester.
sec_user_agent = "Tiran (tiranm11@outlook.com)"
sec_delay_s    = 0.60
MAX_TRIES      = 6
BASE_SLEEP     = 1.0
TIMEOUT_JSON_S = 90
TIMEOUT_DL_S   = 180

# -----------------------
# 3) Create output folders and log paths
# -----------------------
OUT_ROOT.mkdir(parents=True, exist_ok=True)
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
# 4) Pause and stop controls
# -----------------------
# Create a file named PAUSE in OUT_ROOT to pause the run.
# Create a file named STOP in OUT_ROOT to stop the run safely.
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
# 5) Optional clean restart
# -----------------------
# This only deletes the log files. It does not delete downloaded filings.
if fresh_start:
  print("FRESH START: deleting prior logs and starting clean.")
for f in [log_file_fund, log_file_detail, log_file_move]:
  if f.exists():
  f.unlink()

# -----------------------
# 6) CSV logging helpers
# -----------------------
# Every filing-level event is written to the detail log using these fields.
DETAIL_FIELDS = [
  "crsp_portno","ticker","fund_name",
  "cik_type","cik",
  "accession","filingDate","reportDate",
  "status","detail",
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
# 7) SEC request helpers
# -----------------------
# These helpers add retry logic for temporary SEC/network errors.
class NonRetryableHTTPError(RuntimeError):
  pass

session = requests.Session()
session.headers.update({
  "User-Agent": sec_user_agent,
  "Accept-Encoding": "gzip, deflate",
})

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
# 8) Check whether a filing folder already has N-CSR HTML
# -----------------------
# This helps the resume logic decide whether a filing already has the needed HTML.
def folder_has_html(p: Path) -> bool:
  """
    True ONLY if folder contains an HTML/HTM file with 'ncsr' in the filename.
    This matches the detector's pick_ncsr_html logic.
    """
if not p.exists():
  return False
try:
  for fp in p.rglob("*"):
  if fp.is_file() and fp.suffix.lower() in (".htm", ".html") and ("ncsr" in fp.name.lower()):
  return True
return False
except Exception:
  return False

# -----------------------
# 9) HTML-to-PDF rendering helpers
# -----------------------
# These functions use Playwright to render a local HTML filing into a clean PDF.
def _run_coro(coro):
  """
    Run an async coroutine safely. On a normal script there is no running loop,
    but we keep this compatible with your prior notebook-safe approach.
    """
try:
  loop = asyncio.get_event_loop()
if loop.is_running():
  return loop.run_until_complete(coro)
return loop.run_until_complete(coro)
except RuntimeError:
  return asyncio.run(coro)

async def _render_html_to_pdf_async(html_path: Path, pdf_path: Path) -> bool:
  try:
  async with async_playwright() as p:
  browser = await p.chromium.launch(headless=True)
page = await browser.new_page()

page.set_default_navigation_timeout(180_000)
page.set_default_timeout(180_000)

file_url = html_path.resolve().as_uri()

try:
  await page.goto(file_url, wait_until="load", timeout=180_000)
except Exception:
  await page.goto(file_url, wait_until="domcontentloaded", timeout=180_000)

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
  if not MAKE_RENDERED_PDF:
  return False
return _run_coro(_render_html_to_pdf_async(html_path, pdf_path))

# -----------------------
# 10) Load the TSR manual search list
# -----------------------
# The CSV is read as strings so CIKs keep their leading zeros.
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
# 11) Load SEC mutual fund map for class/series matching
# -----------------------
# This map is used only to help verify whether a downloaded filing matches the target fund.
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
# 12) General helper functions
# -----------------------
# These helpers handle filenames, folder paths, SEC filing metadata, assets, and fund matching.
def pad10(x: int) -> str:
  return f"{int(x):010d}"

def _dates_to_pydate(values) -> pd.Series:
  return pd.Series(pd.to_datetime(values, errors="coerce")).dt.date

def safe_filename(x: str) -> str:
  x = unidecode(str(x))
x = re.sub(r"[^A-Za-z0-9._-]+", "_", x)
    return x[:180]

def rendered_pdf_path_for_filing(filing_dir: Path, cik: int, accession: str) -> Path:
    acc_nodash = str(accession).replace("-", "")
    name = f"NCSR Report-{pad10(cik)}-{acc_nodash}.pdf"
    return filing_dir / safe_filename(name)

def folder_has_any_real_files(p: Path) -> bool:
    if not p.exists():
        return False
    marker = p / ".download_complete"
    if marker.exists():
        return True
    for ext in (".htm", ".html", ".pdf", ".xml"):
        try:
            if any(p.rglob(f"*{ext}")):
                return True
        except Exception:
            pass
    return False

def filing_dir_candidates(cik: int, acc: str, period_lab: str) -> List[Path]:
    cik_root = OUT_RAW / f"CIK{pad10(cik)}"
    acc_nodash = acc.replace("-", "")

    stable = cik_root / acc_nodash
    legacy = cik_root / period_lab / acc_nodash

    if USE_STABLE_FILING_DIR:
        out = [stable, legacy]
    else:
        out = [legacy]
        if cik_root.exists():
            try:
                for d in cik_root.iterdir():
                    if d.is_dir() and d.name[:4].isdigit():
                        out.append(d / acc_nodash)
            except Exception:
                pass

    seen, uniq = set(), []
    for p in out:
        ps = str(p)
        if ps not in seen:
            seen.add(ps)
            uniq.append(p)
    return uniq

def choose_filing_dir(cik: int, acc: str, period_lab: str) -> Path:
    cands = filing_dir_candidates(cik, acc, period_lab)
    for d in cands:
        if folder_has_any_real_files(d):
            return d

    cik_root = OUT_RAW / f"CIK{pad10(cik)}"
    acc_nodash = acc.replace("-", "")

    if USE_STABLE_FILING_DIR:
        return cik_root / acc_nodash
    else:
        return cik_root / period_lab / acc_nodash

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
# 13) Resume state from existing logs
# -----------------------
# These sets let the script skip filings that were already downloaded or already matched.
evaluated_key_set = set()
downloaded_key_set = set()

if (not fresh_start) and log_file_detail.exists():
    try:
        tmp = pd.read_csv(log_file_detail, dtype=str)
        need = {"crsp_portno","cik","accession","status"}
        if need.issubset(set(tmp.columns)):
            dl_statuses = {
                "DOWNLOADED",
                "ALREADY_EXISTS",
                "RENDERED_PDF",
                "SKIP_DOWNLOAD_ALREADY_HAVE_FOLDER",
                "SKIP_DOWNLOAD_ALREADY_HAVE_HTML",
            }
            tmp2 = tmp[tmp["status"].isin(dl_statuses) & tmp["accession"].notna()].copy()
            for _, r in tmp2.iterrows():
                try:
                    downloaded_key_set.add(f"{int(r['crsp_portno'])}|{int(r['cik'])}|{str(r['accession'])}")
                except Exception:
                    pass
    except Exception:
        pass

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
# 14) Fund-level summary log
# -----------------------
# This table gives one row per fund and is refreshed throughout the run.
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
# 15) Main download loop
# -----------------------
# The loop works fund by fund. For each fund, it tries available CIKs in order,
# finds N-CSR/N-CSR/A filings, downloads likely report documents, and logs outcomes.
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
    if (not FORCE_RECHECK_DONE_FUNDS) and len(cur_status) and cur_status.iloc[0] == "DONE":
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

        if acc_key in downloaded_key_set:
            period_dt  = rdt if pd.notna(rdt) else fdt
            period_lab = pd.to_datetime(period_dt).strftime("%Y-%m-%d")
            filing_dir = choose_filing_dir(cik, acc, period_lab)

            print("    DEBUG html_any =", any(filing_dir.rglob("*.htm")) or any(filing_dir.rglob("*.html")), flush=True)
            print("    DEBUG html_ncsr =", folder_has_html(filing_dir), flush=True)

            if folder_has_html(filing_dir):
                print("  Already downloaded earlier (and HTML exists). Skipping:", acc, flush=True)
                continue
            else:
                print("  Repair mode: log says downloaded, but HTML missing -> will attempt HTML download:", acc, flush=True)

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

        native_pdf_in_candidates = False
        if SKIP_RENDER_IF_NATIVE_PDF_EXISTS and candidates.shape[0] > 0:
            try:
                native_pdf_in_candidates = candidates["name"].astype(str).str.lower().str.endswith(".pdf").any()
            except Exception:
                native_pdf_in_candidates = False

        cik_dir = str(int(cik))
        acc_nodash = acc.replace("-", "")

        filing_dir = choose_filing_dir(cik, acc, period_lab)
        filing_dir.mkdir(parents=True, exist_ok=True)

        already_have_files = folder_has_any_real_files(filing_dir)
        already_have_html  = folder_has_html(filing_dir)

        if already_have_files and SKIP_DOWNLOAD_IF_FOLDER_HAS_FILES and already_have_html:
            append_row_csv(log_file_detail, {
                "crsp_portno": crsp_portno, "ticker": ticker, "fund_name": fund_name,
                "cik_type": cik_type_used, "cik": cik,
                "accession": acc, "filingDate": fdt, "reportDate": rdt,
                "status": "SKIP_DOWNLOAD_ALREADY_HAVE_HTML",
                "detail": str(filing_dir),
                "sec_filing_dir_url": filing_dir_url, "sec_index_json_url": index_json_url, "sec_doc_url": ""
            }, DETAIL_FIELDS)

        rendered_pdf_done_for_this_filing = False

        if not (already_have_files and SKIP_DOWNLOAD_IF_FOLDER_HAS_FILES and already_have_html):
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

                    download_html_assets_and_rewrite(dest, cik_dir, acc_nodash, filing_dir)

                    should_render_this_doc = (
                        MAKE_RENDERED_PDF and
                        (dest.suffix.lower() in (".htm", ".html")) and
                        (not rendered_pdf_done_for_this_filing)
                    )

                    if should_render_this_doc and RENDER_ONLY_PRIMARY_DOC:
                        should_render_this_doc = (pdoc.strip().lower() == docname.strip().lower())

                    if should_render_this_doc and SKIP_RENDER_IF_NATIVE_PDF_EXISTS and native_pdf_in_candidates:
                        should_render_this_doc = False

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

        if WRITE_DOWNLOAD_MARKER and folder_has_any_real_files(filing_dir):
            try:
                (filing_dir / ".download_complete").write_text(
                    f"completed {datetime.now().isoformat()}",
                    encoding="utf-8"
                )
            except Exception:
                pass

        downloaded_key_set.add(acc_key)

        if use_relevance_filter:
            fund_kw = extract_fund_keywords(fund_name, k=3)

            high_fund = []
            for v in [row.get("classid"), row.get("seriesid"), ticker,
                      row.get("series_cik"), row.get("contract_cik"), row.get("comp_cik")]:
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
# 16) Final log and completion message
# -----------------------
# Fill in any unfinished statuses, write the final fund log, and print output locations.
fund_summary.loc[(fund_summary["status"].isna()) | (fund_summary["status"] == ""), "status"] = \
    fund_summary["crsp_portno"].apply(lambda x: "NOT_MATCHED_OR_SKIPPED" if int(x) in fund_run_ids else "NOT_RUN")

fund_summary.loc[(fund_summary["ticker"].isna()) | (fund_summary["ticker"].astype(str).str.strip() == ""),
                 "note"] = "Missing/blank ticker in tsr_list"

write_fund_log()

print("DONE.")
print("Fund-wise log saved to: ", log_file_fund)
print("Detail log saved to:    ", log_file_detail)
print("Files saved under:      ", OUT_RAW)
print("Output root:            ", OUT_ROOT)