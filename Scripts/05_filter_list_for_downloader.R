library(data.table)
library(lubridate)
# =====================================================================
# ==== 0) Preconditions + make sure we're using data.table ====
# =====================================================================
stopifnot(exists("dt"))

dt_analysis <- copy(dt)
assign("dt_analysis", dt_analysis, envir = .GlobalEnv)

setDT(dt_analysis)

# =====================================================================
# ==== 1) Output paths (where the CSV will land) ====
# =====================================================================
# Keep paths centralized so it's easy to change later if you move the project.
if (!exists("proj_root")) {
  proj_root <- normalizePath(
    file.path(Sys.getenv("USERPROFILE"), "Desktop", "Research Github", "AccountingResearch"),
    winslash = "/", mustWork = TRUE
  )
}

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
  "series_cik","contract_cik","comp_cik",
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
# ---- CIK coverage score (primary ranking) ----

cik_cols <- intersect(c("series_cik","contract_cik","comp_cik"), names(dt0))
for (cc in cik_cols) dt0[, (cc) := fifelse(is.na(get(cc)), "", trimws(as.character(get(cc))))]

dt0[, cik_count := 0L]
if (length(cik_cols) > 0) {
  dt0[, cik_count := Reduce(`+`, lapply(cik_cols, function(cc) as.integer(nzchar(get(cc)))))]
}

# ---- Now rank primarily by cik_count, then your tie-breakers ----
setorder(dt0, crsp_portno, -cik_count, -caldt, -has_website, -retail_flag)







# .SD[1] after ordering gives us the top-ranked row within each crsp_portno
tsr_list <- dt0[, .SD[1], by = crsp_portno]

# Drop the helper columns — they were only used for sorting
tsr_list[, c("website_clean","has_website","retail_flag") := NULL]

# Make crsp_portno look integer-ish (nice for CSV readability + downstream merges)
tsr_list[, crsp_portno := as.integer(crsp_portno)]

# drop helper columns
tsr_list[, cik_count := NULL]

# =====================================================================
# ==== 5) Export CSV + quick sanity prints ====
# =====================================================================
# bom=TRUE helps Excel open the CSV cleanly (especially if any weird characters show up)
fwrite(tsr_list, out_csv, sep = ",", quote = TRUE, na = "", bom = TRUE)

cat("Saved:", out_csv, "\n")
cat("Rows (unique crsp_portno):", nrow(tsr_list), "\n")

cik_cols2 <- intersect(c("series_cik","contract_cik","comp_cik"), names(tsr_list))
if (length(cik_cols2) > 0) {
  cat("Funds with any CIK:",
      tsr_list[
        Reduce(`|`, lapply(cik_cols2, function(cc) nzchar(trimws(as.character(get(cc)))))),
        .N
      ], "\n")
} else {
  cat("Funds with any CIK: cannot compute (no CIK columns present)\n")
}
 
# =====================================================================
# ==== PAUSE: wait up to 1 hour for YES, otherwise auto-continue ====
# =====================================================================
cat("\n============================================================\n")
cat("PAUSE: Downloader list has been created:\n  ", out_csv, "\n", sep = "")
cat("Run your Python/Colab downloader now.\n")
cat("If you type YES within 1 hour, the script continues immediately.\n")
cat("If you do nothing, it will automatically continue after 1 hour.\n")
cat("============================================================\n")

if (!requireNamespace("R.utils", quietly = TRUE)) {
  install.packages("R.utils")
}

ans <- ""
got_input <- TRUE

# ---- timeout read (1 hour) ----
ans <- ""
got_input <- TRUE

tryCatch({
  ans <- R.utils::withTimeout(
    readline(prompt = "Type YES to continue now (or wait 1 hour to auto-continue): "),
    timeout = 3600,
    onTimeout = "error"
  )
}, error = function(e) {
  # If it's a timeout, auto-continue
  if (inherits(e, "TimeoutException")) {
    got_input <<- FALSE
    return(invisible(NULL))
  }
  # Any other error: also auto-continue (or stop, your choice)
  got_input <<- FALSE
  return(invisible(NULL))
})

ans <- toupper(trimws(ans))

if (got_input && ans == "YES") {
  cat("Confirmed (YES). Continuing immediately...\n\n")
} else {
  cat("No YES received within 1 hour (or no valid input). Auto-continuing...\n\n")
}
# =====================================================================
# ==== 6) Attach first TSR report/filing dates (ML output) to full dt ====
# =====================================================================

stopifnot(exists("dt"), exists("proj_root"))
setDT(dt)


# Path to the ML output in your project
tsr_dates_path <- file.path(proj_root, "Distrbution Dates", "tsr_first_tsr_dates_by_fund_ML_FIXED.csv")
if (!file.exists(tsr_dates_path)) stop("TSR dates file not found: ", tsr_dates_path)

tsr_dates <- fread(tsr_dates_path)
stopifnot("crsp_portno" %in% names(tsr_dates))

# Keep only what you need (plus accession/cik optional)
keep_tsr_cols <- c("crsp_portno","tsr_filingdate_first","tsr_reportdate_first","cik","accession")
keep_tsr_cols <- keep_tsr_cols[keep_tsr_cols %in% names(tsr_dates)]
tsr_dates <- tsr_dates[, ..keep_tsr_cols]

# Parse dates safely
tsr_dates[, tsr_filingdate_first := as.Date(tsr_filingdate_first, format = "%m/%d/%Y")]
tsr_dates[, tsr_reportdate_first := as.Date(tsr_reportdate_first, format = "%m/%d/%Y")]
cat("Non-missing filing dates in ML file:", sum(!is.na(tsr_dates$tsr_filingdate_first)), "\n")
# Safety: if duplicates exist, keep the earliest filing date per portno (and its report date)
setorder(tsr_dates, crsp_portno, tsr_filingdate_first, tsr_reportdate_first)
tsr_dates <- tsr_dates[, .SD[1], by = crsp_portno]

# Attach to FULL dt (not windowed) — this updates every month for the fund
dt[tsr_dates,
   `:=`(
     tsr_filingdate_first = i.tsr_filingdate_first,
     tsr_reportdate_first = i.tsr_reportdate_first
   ),
   on = .(crsp_portno)
]

# Optional: also carry accession/cik for traceability (only if present)
if ("cik" %in% names(tsr_dates)) {
  dt[tsr_dates, tsr_cik_first := i.cik, on = .(crsp_portno)]
}
if ("accession" %in% names(tsr_dates)) {
  dt[tsr_dates, tsr_accession_first := i.accession, on = .(crsp_portno)]
}

assign("dt", dt, envir = .GlobalEnv)

cat("[05] Attached tsr_filingdate_first + tsr_reportdate_first to full dt (by crsp_portno).\n")
cat("[05] Funds with TSR dates attached:",
    dt[!is.na(tsr_filingdate_first), uniqueN(crsp_portno)], "\n")  

# =====================================================================
# ==== 7) Descriptive stats (CSV outputs) for TSR date coverage & timing ====
#      (Saved to Descriptive Statistics folder; NOT Script 02 Excel)
# =====================================================================

desc_dir <- file.path(proj_root, "Descriptive Statistics")
dir.create(desc_dir, recursive = TRUE, showWarnings = FALSE)

# ---- Build a fund-level snapshot (ONE row per crsp_portno) ----
# We use the most recent caldt row per portno as "fund characteristics".
dt_fund <- copy(dt)
dt_fund[, caldt := as.Date(caldt)]
setorder(dt_fund, crsp_portno, -caldt)
dt_fund <- dt_fund[!is.na(crsp_portno), .SD[1], by = crsp_portno]

# Handy flags
dt_fund[, has_tsr_filing := !is.na(tsr_filingdate_first)]
dt_fund[, has_tsr_report := !is.na(tsr_reportdate_first)]

# Any CIK flag (based on fields your downloader uses)
for (cc in c("series_cik","contract_cik","comp_cik")) {
  if (!(cc %in% names(dt_fund))) dt_fund[, (cc) := ""]
  dt_fund[, (cc) := fifelse(is.na(get(cc)), "", trimws(as.character(get(cc))))]
}
dt_fund[, has_any_cik := (nzchar(series_cik) | nzchar(contract_cik) | nzchar(comp_cik))]

# Has website flag
if (!("website" %in% names(dt_fund))) dt_fund[, website := ""]
dt_fund[, website := fifelse(is.na(website), "", trimws(as.character(website)))]
dt_fund[, has_website := nzchar(website)]

# Retail flag (same logic you use)
if (!all(c("retail_fund","inst_fund") %in% names(dt_fund))) {
  dt_fund[, retail_fund := NA_character_]
  dt_fund[, inst_fund := NA_character_]
}
dt_fund[, retail_flag := (toupper(trimws(as.character(retail_fund))) == "Y") &
          (toupper(trimws(as.character(inst_fund))) != "Y")]

# ============================================================
# A) TSR coverage table (counts + %)
# ============================================================
A_tsr_coverage <- dt_fund[, .(
  n_funds_total = .N,
  n_with_tsr_filing = sum(has_tsr_filing),
  n_missing_tsr_filing = sum(!has_tsr_filing),
  pct_with_tsr_filing = 100 * mean(has_tsr_filing),
  pct_missing_tsr_filing = 100 * mean(!has_tsr_filing),
  n_with_tsr_report = sum(has_tsr_report),
  n_missing_tsr_report = sum(!has_tsr_report),
  n_filing_yes_report_no = sum(has_tsr_filing & !has_tsr_report),
  n_report_yes_filing_no = sum(!has_tsr_filing & has_tsr_report)
)]
fwrite(A_tsr_coverage, file.path(desc_dir, "TSR_A_coverage_counts_pct.csv"))

# ============================================================
# B) Timing lag distribution (filing - report, in days)
# ============================================================
B_tsr_lag <- dt_fund[has_tsr_filing & has_tsr_report][
  , .(
    n = .N,
    lag_days_min = min(as.integer(tsr_filingdate_first - tsr_reportdate_first), na.rm = TRUE),
    lag_days_p10 = as.numeric(quantile(as.integer(tsr_filingdate_first - tsr_reportdate_first), 0.10, na.rm = TRUE)),
    lag_days_p50 = as.numeric(quantile(as.integer(tsr_filingdate_first - tsr_reportdate_first), 0.50, na.rm = TRUE)),
    lag_days_p90 = as.numeric(quantile(as.integer(tsr_filingdate_first - tsr_reportdate_first), 0.90, na.rm = TRUE)),
    lag_days_max = max(as.integer(tsr_filingdate_first - tsr_reportdate_first), na.rm = TRUE)
  )
]
fwrite(B_tsr_lag, file.path(desc_dir, "TSR_B_lag_filing_minus_report_days.csv"))

# ============================================================
# C) Adoption over time (fund counts by filing month)
# ============================================================
# Convert filing date -> month bucket (month-end)
dt_fund[, tsr_filing_monthend := as.Date(ceiling_date(tsr_filingdate_first, "month") - days(1))]
C_tsr_adoption <- dt_fund[has_tsr_filing == TRUE,
                          .(n_funds_first_tsr = .N),
                          by = .(tsr_filing_monthend)
][order(tsr_filing_monthend)]
fwrite(C_tsr_adoption, file.path(desc_dir, "TSR_C_adoption_by_filing_month.csv"))

# ============================================================
# D) With vs missing TSR filing date (selection/bias check)
# ============================================================
# Use a few stable characteristics; include only if present.
vars_num <- c("mtna", "mret", "exp_ratio", "turn_ratio")
vars_num <- vars_num[vars_num %in% names(dt_fund)]

# Build summary: means of numeric vars + shares of key flags
D_compare <- dt_fund[, c(
  .(
    n_funds = .N,
    pct_has_any_cik = 100 * mean(has_any_cik),
    pct_has_website = 100 * mean(has_website),
    pct_retail_flag = 100 * mean(retail_flag)
  ),
  as.list(setNames(lapply(.SD, function(x) mean(as.numeric(x), na.rm=TRUE)),
                   paste0("mean_", vars_num)))
), by = .(has_tsr_filing), .SDcols = vars_num]

setorder(D_compare, -has_tsr_filing)
fwrite(D_compare, file.path(desc_dir, "TSR_D_with_vs_missing_characteristics.csv"))

cat("[05] Wrote TSR descriptives CSVs to: ", desc_dir, "\n", sep="")

script6 <- file.path(proj_root, "Scripts", "06_Create Samples.R")
if (!file.exists(script6)) stop("Script 06 not found at: ", script6)
source(script6)