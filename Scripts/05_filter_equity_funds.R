library(data.table)

# =====================================================================
# ==== 0) Preconditions + make sure we're using data.table ====
# =====================================================================
# This script expects `dt_analysis` to already exist in memory.
stopifnot(exists("dt_analysis"))
setDT(dt_analysis)

# =====================================================================
# ==== 1) Output paths (where the CSV will land) ====
# =====================================================================
# Keep paths centralized so it's easy to change later if you move the project.
proj_root <- normalizePath(
  file.path(Sys.getenv("USERPROFILE"), "Desktop", "Research Github", "AccountingResearch"),
  winslash = "/", mustWork = TRUE
)

out_dir <- file.path(proj_root, "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# This is the manual-search / EDGAR-downloader input file we’re generating
out_csv <- file.path(out_dir, "TSR Manual Search List - Equity.csv")

# =====================================================================
# ==== 2) Column selection: keep only what we need for search + downloader ====
# =====================================================================
# These are the core identifiers + CIKs + website fields used for:
# - manual “TSR” website searching
# - the EDGAR download automation (CIK-driven)
keep_cols <- c(
  "crsp_portno","crsp_fundno","ticker","fund_name",
  "series_cik","contract_cik","comp_cik","cik_best",
  "website","city","state",
  "crsp_cl_grp","retail_fund","inst_fund",
  "caldt"
)

# Some runs might not have all columns — so we intersect with what's actually present
keep_cols <- keep_cols[keep_cols %in% names(dt_analysis)]

# Pull a slim working table (keeps everything lighter/faster)
dt0 <- dt_analysis[, ..keep_cols]

# =====================================================================
# ==== 3) Light cleaning + helper flags for row-picking ====
# =====================================================================
# Standardize types/strings so ordering/filters behave consistently
dt0[, caldt := as.Date(caldt)]
dt0[, ticker := toupper(trimws(as.character(ticker)))]

# Website can be NA/blank; we normalize to "" and create an indicator
dt0[, website_clean := fifelse(is.na(website), "", trimws(as.character(website)))]
dt0[, has_website := website_clean != ""]

# "Retail-only" flag: retail_fund == Y AND inst_fund != Y
# (So a fund marked both retail and institutional doesn’t get treated as retail-only.)
dt0[, retail_flag := (toupper(trimws(as.character(retail_fund))) == "Y") &
      (toupper(trimws(as.character(inst_fund))) != "Y")]

# =====================================================================
# ==== 4) Choose ONE representative row per portfolio (crsp_portno) ====
# =====================================================================
# Selection priority (in order):
#   1) has website (helps manual search + fund lookup)
#   2) retail_flag (retail share classes are usually better for TSR availability)
#   3) most recent caldt (prefer the latest snapshot)
setorder(dt0, crsp_portno, -has_website, -retail_flag, -caldt)

# .SD[1] after ordering gives us the top-ranked row within each crsp_portno
tsr_list <- dt0[, .SD[1], by = crsp_portno]

# Drop the helper columns — they were only used for sorting
tsr_list[, c("website_clean","has_website","retail_flag") := NULL]

# Make crsp_portno look integer-ish (nice for CSV readability + downstream merges)
tsr_list[, crsp_portno := as.integer(crsp_portno)]

# =====================================================================
# ==== 5) Export CSV + quick sanity prints ====
# =====================================================================
# bom=TRUE helps Excel open the CSV cleanly (especially if any weird characters show up)
fwrite(tsr_list, out_csv, sep = ",", quote = TRUE, na = "", bom = TRUE)

cat("Saved:", out_csv, "\n")
cat("Rows (unique crsp_portno):", nrow(tsr_list), "\n")

# Quick check: how many rows have at least one CIK field populated?
cat("Funds with any CIK:",
    tsr_list[
      (nzchar(series_cik) | nzchar(contract_cik) | nzchar(comp_cik)),
      .N
    ], "\n")
