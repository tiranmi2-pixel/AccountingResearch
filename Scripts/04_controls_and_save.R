# ===================== 04_controls_and_save.R =====================
# PURPOSE:
#  1) Add control variables to dt in memory
#  2) DO NOT reorder existing columns; only append new control columns to the right
#  3) Save final CSV (only here)
#
# CHANGE (per your request):
#  - Only attach / compute these controls:
#      1) log_tna_l1  (computed from dt: mtna)
#      2) turn_ratio_l1 (extracted from crspq.fund_summary2 -> turn_ratio, then lag)
#      3) mgmt_fee_l1   (extracted from crspq.fund_summary2 -> mgmt_fee, then lag)
#  - Do NOT search for alternative tables (no crsp.fund_summary*, no front/rear loads).
#  - Keep everything else the same (ordering preservation, save, source Script 05).

library(data.table)
library(DBI)
library(RPostgres)

stopifnot(exists("dt"), exists("wrds"), exists("proj_root"))
setDT(dt)

# -----------------------
# add_controls_monthly
# -----------------------
add_controls_monthly <- function(dt,
                                 wrds = NULL,
                                 connect_if_null = TRUE,
                                 wrds_user = "tiran",
                                 include_loads = TRUE) {  # kept for compatibility, not used now
  
  stopifnot(is.data.table(dt))
  
  # Preserve incoming column order exactly
  old_cols <- names(dt)
  
  req <- c("crsp_fundno","crsp_portno","caldt","mret","mtna","first_offer_dt","mgmt_cd")
  missing_req <- setdiff(req, names(dt))
  if (length(missing_req) > 0) {
    stop("dt is missing required columns: ", paste(missing_req, collapse = ", "))
  }
  
  # Types + order
  dt[, caldt := as.Date(caldt)]
  dt[, first_offer_dt := as.Date(first_offer_dt)]
  dt[, crsp_fundno := as.numeric(crsp_fundno)]
  dt[, crsp_portno := as.numeric(crsp_portno)]
  dt[, mgmt_cd := as.numeric(mgmt_cd)]
  dt[, mret := as.numeric(mret)]
  dt[, mtna := as.numeric(mtna)]
  if (is.finite(mean(abs(dt$mret), na.rm=TRUE)) && mean(abs(dt$mret), na.rm=TRUE) > 1) dt[, mret := mret/100]
  setorder(dt, crsp_fundno, caldt)
  
  # -----------------------
  # A) Controls from panel (ONLY log_tna_l1)
  # -----------------------
  # 1) Age (years since first offer date), then lag
  dt[, age_years := as.numeric(caldt - first_offer_dt) / 365.25]
  dt[, age_years_l1 := shift(age_years, 1L), by = crsp_fundno]
  
  # 2) Lagged log TNA
  dt[, log_tna_l1 := shift(log1p(mtna), 1L), by = crsp_fundno]
  
  # 3) Flow and lagged flow
  if (!("mtna_l1" %in% names(dt))) {
    dt[, mtna_l1 := shift(mtna, 1L), by = crsp_fundno]
  }
  
  dt[, flow_l1 := shift(flow, 1L), by = crsp_fundno]
  
  # 4) Family TNA (by management company x month), then log
  dt[, family_tna_l1 := {
    if (all(is.na(mtna_l1))) NA_real_ else sum(mtna_l1, na.rm = TRUE)
  }, by = .(mgmt_cd, caldt)]
  
  dt[, log_familytna_l1 := fifelse(
    is.na(family_tna_l1) | family_tna_l1 <= 0,
    NA_real_,
    log(family_tna_l1)
  )]
  # -----------------------
  # B) Extract from CRSPQ.fund_summary2 (ONLY turn_ratio + mgmt_fee) and create lags
  #     - No searching for alternative tables
  # -----------------------
  # -----------------------
  # B) Extract from crsp_q_mutualfunds.fund_summary2:
  #    exp_ratio + turn_ratio + mgmt_fee (as-of / roll forward), then lag
  # -----------------------
  if (!is.null(wrds)) {
    
    if (!DBI::dbIsValid(wrds)) {
      message("[04] WRDS connection invalid — reconnecting...")
      wrds <- DBI::dbConnect(
        RPostgres::Postgres(),
        host = "wrds-pgdata.wharton.upenn.edu",
        port = 9737,
        dbname = "wrds",
        sslmode = "require",
        user = wrds_user
      )
    }
    
    fs2_cols <- dbGetQuery(wrds, "
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='crsp_q_mutualfunds'
      AND table_name='fund_summary2'
  ")$column_name
    
    need <- c("crsp_portno","caldt","turn_ratio","mgmt_fee","exp_ratio")
    miss <- setdiff(need, fs2_cols)
    if (length(miss) > 0) stop("crsp_q_mutualfunds.fund_summary2 missing: ", paste(miss, collapse=", "))
    
    start_date <- min(dt$caldt, na.rm = TRUE)
    end_date   <- max(dt$caldt, na.rm = TRUE)
    
    fs2 <- dbGetQuery(wrds, sprintf("
    SELECT crsp_portno,
           caldt,
           turn_ratio,
           mgmt_fee,
           exp_ratio
    FROM crsp_q_mutualfunds.fund_summary2
    WHERE caldt BETWEEN '%s' AND '%s'
  ", format(start_date, "%Y-%m-%d"), format(end_date, "%Y-%m-%d")))
    
    fs2 <- as.data.table(fs2)
    fs2[, caldt := as.Date(caldt)]
    fs2[, crsp_portno := as.numeric(crsp_portno)]
    fs2[, turn_ratio := as.numeric(turn_ratio)]
    fs2[, mgmt_fee   := as.numeric(mgmt_fee)]
    fs2[, exp_ratio  := as.numeric(exp_ratio)]
    
    # de-dup: one row per (portno, caldt)
    setorder(fs2, crsp_portno, caldt)
    fs2 <- fs2[, .SD[.N], by = .(crsp_portno, caldt)]
    
    # Convert percent -> decimal if needed
    if (is.finite(mean(fs2$turn_ratio, na.rm=TRUE)) && mean(fs2$turn_ratio, na.rm=TRUE) > 1) fs2[, turn_ratio := turn_ratio/100]
    if (is.finite(mean(fs2$mgmt_fee,   na.rm=TRUE)) && mean(fs2$mgmt_fee,   na.rm=TRUE) > 1) fs2[, mgmt_fee   := mgmt_fee/100]
    if (is.finite(mean(fs2$exp_ratio,  na.rm=TRUE)) && mean(fs2$exp_ratio,  na.rm=TRUE) > 1) fs2[, exp_ratio  := exp_ratio/100]
    
    setkey(dt,  crsp_portno, caldt)
    setkey(fs2, crsp_portno, caldt)
    
    # as-of join (roll forward within portno)
    fs2_match <- fs2[dt, on=.(crsp_portno, caldt), roll=TRUE, mult="last",
                     .(crsp_portno, caldt, turn_ratio, mgmt_fee, exp_ratio)]
    
    dt[fs2_match, `:=`(
      turn_ratio = i.turn_ratio,
      mgmt_fee   = i.mgmt_fee,
      exp_ratio  = i.exp_ratio
    ), on=.(crsp_portno, caldt)]
    
    # Lags (keep consistent with your current design: by crsp_portno)
    dt[, turn_ratio_l1 := shift(turn_ratio, 1L), by = crsp_portno]
    dt[, mgmt_fee_l1   := shift(mgmt_fee,   1L), by = crsp_portno]
    dt[, exp_ratio_l1  := shift(exp_ratio,  1L), by = crsp_portno]
    
  } else {
    dt[, `:=`(
      turn_ratio    = NA_real_,
      turn_ratio_l1 = NA_real_,
      mgmt_fee      = NA_real_,
      mgmt_fee_l1   = NA_real_,
      exp_ratio     = NA_real_,
      exp_ratio_l1  = NA_real_
    )]
  }
  
  #-------------------------------------------------
  # Preserve existing order; append ONLY new columns to the right.
  # -----------------------
  new_cols <- setdiff(names(dt), old_cols)
  setcolorder(dt, c(old_cols, new_cols))
  
  dt[]
}

# -----------------------
# Run controls (THIS MUST BE OUTSIDE THE FUNCTION)
# -----------------------
dt <- add_controls_monthly(dt, wrds = wrds, connect_if_null = FALSE, include_loads = TRUE)
# -----------------------
# [04] Missingness viewer (controls only)
# Place this AFTER add_controls_monthly() is run
# -----------------------
controls_to_check <- c(
  "age_years_l1",
  "log_tna_l1",
  "flow_l1",
  "log_familytna_l1",
  "turn_ratio_l1",
  "mgmt_fee_l1",
  "exp_ratio_l1"
)

miss_view <- rbindlist(lapply(controls_to_check, function(v) {
  if (!(v %in% names(dt))) {
    return(data.table(
      variable = v,
      present = FALSE,
      n = nrow(dt),
      non_missing = NA_integer_,
      missing = nrow(dt),
      pct_missing = 100
    ))
  }
  n <- nrow(dt)
  miss_n <- sum(is.na(dt[[v]]))
  data.table(
    variable = v,
    present = TRUE,
    n = n,
    non_missing = n - miss_n,
    missing = miss_n,
    pct_missing = round(100 * miss_n / n, 3)
  )
}), fill = TRUE)

print(miss_view)


assign("dt", dt, envir = .GlobalEnv)

cat("\n[04] Controls added.\n")
cat("[04] Rows:", nrow(dt), " Cols:", ncol(dt), "\n")

# -----------------------
# SAVE FINAL CSV (ONLY HERE)
# -----------------------
out_dir <- file.path(proj_root, "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# Use same window as Script 02 if available; else default to 2022-2025
WIN_START <- if (exists("WIN_START")) get("WIN_START") else as.Date("2022-01-01")
WIN_END   <- if (exists("WIN_END"))   get("WIN_END")   else as.Date("2025-12-31")

dt_save <- copy(dt)
dt_save[, caldt := as.Date(caldt)]
dt_save <- dt_save[caldt >= WIN_START & caldt <= WIN_END]

csv_file <- file.path(out_dir, "mf_with_names_equity_perf_controls_2022_2025.csv")
fwrite(dt_save, csv_file, sep = ",", quote = TRUE, na = "", bom = TRUE)

cat("[04] Saved CSV to:", csv_file, "\n")
stopifnot(file.exists(csv_file))

# -----------------------
# AUTO-RUN SCRIPT 05
# -----------------------
script5_path <- file.path(proj_root, "Scripts", "05_filter_list_for_downloader.R")
if (!file.exists(script5_path)) stop("Script 05 not found at: ", script5_path)

source(script5_path)