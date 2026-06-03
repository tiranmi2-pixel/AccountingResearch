# ============================================================
# ML-based TSR implementation detector
#
# Note: This is Python code, not R code.
# It is uploaded to GitHub only for information and transparency about how first TSR dates were identified.
#
# What this script does:
#   - Reads the EDGAR downloader detail log.
#   - Finds the downloaded filing folder for each N-CSR/N-CSR/A filing.
#   - Chooses the best filing document to read:
#       1) primaryDocument from the log
#       2) HTML file with "ncsr" in the filename
#       3) largest HTML file
#       4) PDF fallback
#   - Loads the trained TSR HTML classifier.
#   - Scores each filing as TSR or non-TSR.
#   - Saves filing-level TSR predictions.
#   - Saves the first TSR filing/report date for each CRSP portfolio.
# ============================================================

# -----------------------
# 0) Mount Google Drive
# -----------------------
from google.colab import drive
drive.mount("/content/drive")  # harmless if already mounted

import re
from pathlib import Path
import pandas as pd
from datetime import datetime

# -----------------------
# 0) Project paths and output files
# -----------------------
OUT_ROOT = Path("/content/drive/MyDrive/TSR Project/NCSR Reports")
OUT_RAW  = OUT_ROOT / "raw_files"

LOG_DETAIL = OUT_ROOT / "tsr_download_log_detail.csv"

# These output filenames are ML-specific so they do not depend on older phrase-rule runs.
OUT_BY_FILING = OUT_ROOT / "tsr_detected_by_filing_ML.csv"
OUT_BY_FUND   = OUT_ROOT / "tsr_first_tsr_dates_by_fund_ML.csv"

MAX_TEXT_CHARS = 1_500_000     # cap text size (speed + memory)

# -----------------------
# ML model path and threshold
# -----------------------
ML_MODEL_PATH = Path("/content/drive/MyDrive/TSR Project/training_year_labels/tsr_html_classifier.joblib")
ML_PROBA_THRESHOLD = 0.50  # classify TSR if p_tsr >= this

# -----------------------
# Checkpoint frequency
# -----------------------
CHECKPOINT_EVERY = 100  # write OUT_BY_FILING every N newly-processed filings

print("OUT_ROOT       :", OUT_ROOT)
print("OUT_RAW        :", OUT_RAW)
print("LOG            :", LOG_DETAIL)
print("OUT_BY_FILING  :", OUT_BY_FILING)
print("OUT_BY_FUND    :", OUT_BY_FUND)
print("ML_MODEL       :", ML_MODEL_PATH)

# -----------------------
# 1) Install/load packages needed for parsing
# -----------------------
def pip_install_if_missing(pkg, import_name=None):
  import importlib, subprocess, sys
name = import_name or pkg
try:
  importlib.import_module(name)
return
except Exception:
  print(f"Installing {pkg} ...")
subprocess.check_call([sys.executable, "-m", "pip", "-q", "install", pkg])

pip_install_if_missing("beautifulsoup4", "bs4")
pip_install_if_missing("lxml", "lxml")
pip_install_if_missing("pypdf", "pypdf")
pip_install_if_missing("joblib", "joblib")

from bs4 import BeautifulSoup
from pypdf import PdfReader
import joblib

# -----------------------
# Load the trained ML model once
# -----------------------
if not ML_MODEL_PATH.exists():
  raise FileNotFoundError(f"Missing ML model file: {ML_MODEL_PATH}")

ml_model = joblib.load(ML_MODEL_PATH)
print("Loaded ML model OK.")

# -----------------------
# 2) Text extraction helpers
# -----------------------
def norm_text(s: str) -> str:
  if not s:
  return ""
s = s.replace("\x00", " ")
s = s.replace("\u00A0", " ")
s = s.replace("\u2010", "-").replace("\u2011", "-").replace("\u2012", "-")
s = s.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
s = re.sub(r"\s+", " ", s)
return s.strip()

def extract_text_from_html(path: Path) -> str:
  try:
  raw = path.read_text(encoding="utf-8", errors="ignore")
except Exception:
  return ""
soup = BeautifulSoup(raw, "lxml")
for tag in soup(["script", "style", "noscript"]):
  tag.decompose()
text = soup.get_text(" ")
return norm_text(text)[:MAX_TEXT_CHARS]

def extract_text_from_pdf(path: Path) -> str:
  try:
  reader = PdfReader(str(path))
except Exception:
  return ""
out = []
total = 0
for page in reader.pages:
  try:
  t = page.extract_text() or ""
except Exception:
  t = ""
if not t:
  continue
t = norm_text(t)
if not t:
  continue
out.append(t)
total += len(t)
if total >= MAX_TEXT_CHARS:
  break
return " ".join(out)[:MAX_TEXT_CHARS]

def phrase_to_fuzzy_regex(phrase: str) -> re.Pattern:
  tokens = re.findall(r"[A-Za-z0-9]+", phrase)
    if not tokens:
        return re.compile(r"(?!x)x")
    sep = r"[^A-Za-z0-9]+"
    pattern = sep.join(map(re.escape, tokens))
    return re.compile(pattern, flags=re.I)

def find_first_snippet(text_upper: str, phrases, window=160) -> str:
    for ph in phrases:
        pat = phrase_to_fuzzy_regex(ph)
        m = pat.search(text_upper)
        if m:
            i0 = max(0, m.start() - window)
            i1 = min(len(text_upper), m.end() + window)
            return norm_text(text_upper[i0:i1])
    return ""

# -----------------------
# Keep detection focused on Item/Section 1 when possible
# -----------------------
SECTION1_START_PATTERNS = [
    r"\bitem\s*1\b",
    r"\bitem\s*1\s*[:.\-]\b",
    r"\bsection\s*1\b",
    r"\bpart\s*1\b",
]
SECTION1_END_PATTERNS = [
    r"\bitem\s*2\b",
    r"\bitem\s*2\s*[:.\-]\b",
    r"\bsection\s*2\b",
    r"\bpart\s*2\b",
]

def extract_section1_only(text: str) -> str:
    if not text:
        return ""
    tu = text.upper()
    start_idx = None
    for sp in SECTION1_START_PATTERNS:
        m = re.search(sp, tu, flags=re.I)
        if m:
            start_idx = m.start()
            break
    if start_idx is None:
        return text
    end_idx = None
    tu_tail = tu[start_idx:]
    for ep in SECTION1_END_PATTERNS:
        m2 = re.search(ep, tu_tail, flags=re.I)
        if m2:
            end_idx = start_idx + m2.start()
            break
    if end_idx is None or end_idx <= start_idx:
        return text[start_idx : min(len(text), start_idx + 300_000)]
    return text[start_idx:end_idx]

# -----------------------
# ML scoring wrapper
# -----------------------
EVIDENCE_PHRASES_FOR_SNIPPET = [
    "TAILORED SHAREHOLDER REPORT",
    "ANNUAL SHAREHOLDER REPORT",
    "TSR-AR",
    "COSTS OF A $10,000 INVESTMENT",
    "COSTS PAID AS A PERCENTAGE OF A $10,000 INVESTMENT",
]

def tsr_score_ml(text: str):
    if not text:
        return 0.0, "", [], []
    section1_text = extract_section1_only(text)
    try:
        p = float(ml_model.predict_proba([section1_text])[0][1])
    except Exception:
        p = 0.0
    ev = find_first_snippet(section1_text.upper(), EVIDENCE_PHRASES_FOR_SNIPPET)
    hits = [f"ML_PROBA={p:.6f}"]
    anchor_hits = [f"THRESHOLD={ML_PROBA_THRESHOLD:.2f}"]
    return p, ev, hits, anchor_hits

# -----------------------
# 3) Filing folder discovery
# -----------------------
def pad10(x: int) -> str:
    return f"{int(x):010d}"

def accession_nodash(acc: str) -> str:
    return str(acc).replace("-", "").strip()

def folder_has_any_real_files(p: Path) -> bool:
    if not p.exists():
        return False
    if (p / ".download_complete").exists():
        return True
    for ext in (".htm", ".html", ".xml", ".pdf"):
        try:
            if any(p.rglob(f"*{ext}")):
                return True
        except Exception:
            pass
    return False

def find_filing_dir(cik: int, acc: str) -> Path | None:
    cik_root = OUT_RAW / f"CIK{pad10(cik)}"
    if not cik_root.exists():
        return None
    acc_nd = accession_nodash(acc)

    candidates = [cik_root / acc_nd]
    try:
        candidates.extend([p for p in cik_root.glob(f"*/{acc_nd}") if p.is_dir()])
    except Exception:
        pass
    try:
        candidates.extend([p for p in cik_root.rglob(acc_nd) if p.is_dir()])
    except Exception:
        pass

    uniq, seen = [], set()
    for p in candidates:
        sp = str(p)
        if sp not in seen:
            seen.add(sp)
            uniq.append(p)

    for p in uniq:
        if folder_has_any_real_files(p):
            return p
    for p in uniq:
        if p.exists():
            return p
    return None

# -----------------------
# 4) Choose the best document to parse
# -----------------------
def pick_ncsr_html(filing_dir: Path) -> Path | None:
    if not filing_dir or not filing_dir.exists():
        return None
    htmls = [
        p for p in filing_dir.rglob("*")
        if p.is_file()
        and p.suffix.lower() in (".htm", ".html")
        and ("ncsr" in p.name.lower())
    ]
    if not htmls:
        return None
    htmls.sort(key=lambda p: p.stat().st_size if p.exists() else 0, reverse=True)
    return htmls[0]

def pick_largest_html(filing_dir: Path) -> Path | None:
    if not filing_dir or not filing_dir.exists():
        return None
    htmls = [p for p in filing_dir.rglob("*") if p.is_file() and p.suffix.lower() in (".htm", ".html")]
    if not htmls:
        return None
    htmls.sort(key=lambda p: p.stat().st_size if p.exists() else 0, reverse=True)
    return htmls[0]

def pick_any_pdf(filing_dir: Path) -> Path | None:
    if not filing_dir or not filing_dir.exists():
        return None
    pdfs = [p for p in filing_dir.rglob("*.pdf") if p.is_file()]
    if not pdfs:
        return None
    pdfs.sort(key=lambda p: p.stat().st_size if p.exists() else 0, reverse=True)
    return pdfs[0]

def _basename_like(x: str) -> str:
    s = str(x).strip()
    if not s:
        return ""
    s = s.replace("\\", "/")
    return s.split("/")[-1].strip()

def pick_primary_from_log(filing_dir: Path, primary_doc_value: str | None) -> Path | None:
    if not filing_dir or not filing_dir.exists():
        return None
    if not primary_doc_value or str(primary_doc_value).strip() == "":
        return None
    base = _basename_like(primary_doc_value)
    if not base:
        return None
    direct = filing_dir / base
    if direct.exists() and direct.is_file():
        return direct
    base_lower = base.lower()
    try:
        for p in filing_dir.rglob("*"):
            if p.is_file() and p.name.lower() == base_lower:
                return p
    except Exception:
        pass
    return None

def pick_best_doc(filing_dir: Path, primary_doc_value: str | None) -> Path | None:
    p1 = pick_primary_from_log(filing_dir, primary_doc_value)
    if p1 is not None:
        return p1
    p2 = pick_ncsr_html(filing_dir)
    if p2 is not None:
        return p2
    p3 = pick_largest_html(filing_dir)
    if p3 is not None:
        return p3
    p4 = pick_any_pdf(filing_dir)
    if p4 is not None:
        return p4
    return None

# -----------------------
# 6) Load detail log and create the filing list
# -----------------------
if not LOG_DETAIL.exists():
    raise FileNotFoundError(f"Missing log: {LOG_DETAIL}")

log = pd.read_csv(LOG_DETAIL, dtype=str)
log.columns = [c.lower() for c in log.columns]

need_cols = {"crsp_portno","cik","accession","filingdate","reportdate","status"}
missing = need_cols - set(log.columns)
if missing:
    raise ValueError(f"Detail log missing required columns: {sorted(missing)}")

PRIMARY_DOC_COL_CANDIDATES = [
    "primarydocument",
    "primary_document",
    "primary_doc",
    "primary_document_name",
    "primary_document_filename",
]
PRIMARY_DOC_COL = next((c for c in PRIMARY_DOC_COL_CANDIDATES if c in log.columns), None)
print("Primary document column detected:", PRIMARY_DOC_COL)

download_statuses = {
    "DOWNLOADED",
    "ALREADY_EXISTS",
    "RENDERED_PDF",
    "SKIP_DOWNLOAD_ALREADY_HAVE_FOLDER",
    "GOT_FILINGSUMMARY_XML",
    "MATCHED_THIS_FUND",
    "UNVERIFIED_MATCH"
}
work = log[log["status"].isin(download_statuses)].copy()

work = work.dropna(subset=["cik","accession","crsp_portno"]).copy()
work["cik"] = pd.to_numeric(work["cik"], errors="coerce")
work["crsp_portno"] = pd.to_numeric(work["crsp_portno"], errors="coerce")
work = work.dropna(subset=["cik","crsp_portno"]).copy()
work["cik"] = work["cik"].astype(int)
work["crsp_portno"] = work["crsp_portno"].astype(int)

work["filingdate"] = pd.to_datetime(work["filingdate"], errors="coerce")
work["reportdate"] = pd.to_datetime(work["reportdate"], errors="coerce")

work = work.drop_duplicates(subset=["crsp_portno","cik","accession"]).reset_index(drop=True)
print("Unique filings to evaluate:", work.shape[0])

# -----------------------
# 6b) Resume from an existing ML filing-level output
# -----------------------
existing_by_filing = pd.DataFrame()
processed_keys = set()

if OUT_BY_FILING.exists():
    try:
        existing_by_filing = pd.read_csv(OUT_BY_FILING, dtype=str)
        if {"crsp_portno","cik","accession"}.issubset(set(existing_by_filing.columns)):
            for _, rr in existing_by_filing.iterrows():
                try:
                    k = f"{int(rr['crsp_portno'])}|{int(rr['cik'])}|{str(rr['accession']).strip()}"
                    processed_keys.add(k)
                except Exception:
                    pass
        print(f"RESUME: found existing {OUT_BY_FILING.name} with {len(processed_keys)} processed filings.")
    except Exception:
        existing_by_filing = pd.DataFrame()
        processed_keys = set()

def write_checkpoint(existing_df: pd.DataFrame, new_rows: list):
    by_filing_new = pd.DataFrame(new_rows)

    if existing_df.shape[0] > 0:
        by_filing = pd.concat([existing_df, by_filing_new], ignore_index=True)
    else:
        by_filing = by_filing_new

    if by_filing.shape[0] > 0 and {"crsp_portno","cik","accession"}.issubset(set(by_filing.columns)):
        by_filing = by_filing.drop_duplicates(subset=["crsp_portno","cik","accession"], keep="last")

    by_filing["filingdate"] = pd.to_datetime(by_filing["filingdate"], errors="coerce")
    by_filing["reportdate"] = pd.to_datetime(by_filing["reportdate"], errors="coerce")
    by_filing.sort_values(["crsp_portno","cik","filingdate","accession"], inplace=True)

    by_filing.to_csv(OUT_BY_FILING, index=False)
    return by_filing

# -----------------------
# 7) Main ML detection loop
# -----------------------
rows = []
t0 = datetime.now()
new_done = 0

total_remaining_start = 0
for _, rr in work.iterrows():
    k = f"{int(rr['crsp_portno'])}|{int(rr['cik'])}|{str(rr['accession']).strip()}"
    if k not in processed_keys:
        total_remaining_start += 1

remaining = total_remaining_start

for i, r in work.iterrows():
    crsp_portno = int(r["crsp_portno"])
    cik = int(r["cik"])
    acc = str(r["accession"]).strip()

    key = f"{crsp_portno}|{cik}|{acc}"
    if key in processed_keys:
        continue

    remaining -= 1
    fdir = find_filing_dir(cik, acc)

    if fdir is None:
        print(f"[SCAN] {key} | remaining={remaining} | folder=NOT_FOUND | TSR=0", flush=True)
        rows.append({
            "crsp_portno": crsp_portno,
            "cik": cik,
            "accession": acc,
            "filingdate": r["filingdate"],
            "reportdate": r["reportdate"],
            "filing_dir": "",
            "ncsr_html": "",
            "text_len": 0,
            "tsr_score": 0,
            "is_tsr": 0,
            "phrase_hits": "",
            "evidence_snippet": "",
            "anchor_hits": "",
            "note": "FOLDER_NOT_FOUND"
        })
        new_done += 1
    else:
        primary_val = None
        if PRIMARY_DOC_COL is not None:
            try:
                primary_val = r[PRIMARY_DOC_COL]
            except Exception:
                primary_val = None

        doc_path = pick_best_doc(fdir, primary_val)

        if doc_path is None:
            print(f"[SCAN] {key} | remaining={remaining} | folder={fdir} | html=NONE | TSR=0", flush=True)
            rows.append({
                "crsp_portno": crsp_portno,
                "cik": cik,
                "accession": acc,
                "filingdate": r["filingdate"],
                "reportdate": r["reportdate"],
                "filing_dir": str(fdir),
                "ncsr_html": "",
                "text_len": 0,
                "tsr_score": 0,
                "is_tsr": 0,
                "phrase_hits": "",
                "evidence_snippet": "",
                "anchor_hits": "",
                "note": "NO_PRIMARY_NCSR_HTML_HTML_OR_PDF"
            })
            new_done += 1
        else:
            if doc_path.suffix.lower() == ".pdf":
                text = extract_text_from_pdf(doc_path)
            else:
                text = extract_text_from_html(doc_path)

            p_tsr, ev, hits, anchor_hits = tsr_score_ml(text)
            is_tsr = int(p_tsr >= ML_PROBA_THRESHOLD)

            print(f"[SCAN] {key} | remaining={remaining} | folder={fdir} | TSR={is_tsr} | p={p_tsr:.4f}", flush=True)

            rows.append({
                "crsp_portno": crsp_portno,
                "cik": cik,
                "accession": acc,
                "filingdate": r["filingdate"],
                "reportdate": r["reportdate"],
                "filing_dir": str(fdir),
                "ncsr_html": str(doc_path),
                "text_len": int(len(text)),
                "tsr_score": int(round(p_tsr * 10000)),
                "is_tsr": is_tsr,
                "phrase_hits": "; ".join(hits),
                "anchor_hits": "; ".join(anchor_hits),
                "evidence_snippet": ev,
                "note": ""
            })
            new_done += 1

    if new_done > 0 and (new_done % CHECKPOINT_EVERY == 0):
        existing_by_filing = write_checkpoint(existing_by_filing, rows)
        if {"crsp_portno","cik","accession"}.issubset(set(existing_by_filing.columns)):
            processed_keys = set(
                f"{int(rr['crsp_portno'])}|{int(rr['cik'])}|{str(rr['accession']).strip()}"
                for _, rr in existing_by_filing.iterrows()
                if str(rr.get("crsp_portno","")).strip() != ""
            )
        rows = []
        print(f"[CHECKPOINT] Wrote {OUT_BY_FILING.name} after {new_done} new filings.", flush=True)

if rows:
    existing_by_filing = write_checkpoint(existing_by_filing, rows)
    rows = []

by_filing = existing_by_filing.copy()
print("Saved:", OUT_BY_FILING)

# -----------------------
# 8) Create first TSR date by CRSP portfolio
# -----------------------
by_filing["is_tsr"] = pd.to_numeric(by_filing["is_tsr"], errors="coerce").fillna(0).astype(int)
tsr_only = by_filing[by_filing["is_tsr"] == 1].copy()

tsr_only["filingdate"] = pd.to_datetime(tsr_only["filingdate"], errors="coerce")
tsr_only["reportdate"] = pd.to_datetime(tsr_only["reportdate"], errors="coerce")
tsr_only["date_sort"] = tsr_only["filingdate"].where(tsr_only["filingdate"].notna(), tsr_only["reportdate"])

first_by_fund = (
    tsr_only.sort_values(["crsp_portno","date_sort","filingdate","reportdate"])
           .groupby("crsp_portno", as_index=False)
           .first()
)

first_by_fund = first_by_fund.rename(columns={
    "filingdate": "tsr_filingdate_first",
    "reportdate": "tsr_reportdate_first"
})

keep_cols = [
    "crsp_portno",
    "cik",
    "accession",
    "tsr_filingdate_first",
    "tsr_reportdate_first",
    "tsr_score",
    "phrase_hits",
    "anchor_hits",
    "ncsr_html",
    "filing_dir",
    "evidence_snippet"
]
first_by_fund = first_by_fund[keep_cols].copy()

first_by_fund.to_csv(OUT_BY_FUND, index=False)
print("Saved:", OUT_BY_FUND)

dt = datetime.now() - t0
print(f"DONE. Runtime: {dt}")
print("TSR detected filings:", int(by_filing["is_tsr"].sum()))
print("Funds with a TSR first date:", first_by_fund.shape[0])