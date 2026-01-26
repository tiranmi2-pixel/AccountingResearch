# ===================== 04_controls_and_save.R =====================
# PURPOSE:
#  1) Add control variables to dt in memory
#  2) DO NOT reorder existing columns; only append new control columns to the right
#  3) Save final CSV (only here)

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
                                 include_loads = TRUE) {
  
  stopifnot(is.data.table(dt))
  
  # Preserve incoming column order exactly
  old_cols <- names(dt)
  
  req <- c("crsp_fundno","caldt","mret","mtna","first_offer_dt","mgmt_cd")
  missing_req <- setdiff(req, names(dt))
  if (length(missing_req) > 0) {
    stop("dt is missing required columns: ", paste(missing_req, collapse = ", "))
  }
  
  # Types + order
  dt[, caldt := as.Date(caldt)]
  dt[, first_offer_dt := as.Date(first_offer_dt)]
  dt[, crsp_fundno := as.numeric(crsp_fundno)]
  dt[, mgmt_cd := as.numeric(mgmt_cd)]
  dt[, mret := as.numeric(mret)]
  dt[, mtna := as.numeric(mtna)]
  if (is.finite(mean(abs(dt$mret), na.rm=TRUE)) && mean(abs(dt$mret), na.rm=TRUE) > 1) dt[, mret := mret/100]
  setorder(dt, crsp_fundno, caldt)
  
  # -----------------------
  # A) Controls from panel
  # -----------------------
  dt[, age_years := as.numeric(caldt - first_offer_dt) / 365.25]
  dt[, age_years_l1 := shift(age_years, 1L), by = crsp_fundno]
  
  dt[, log_tna := log(pmax(mtna, 1e-6))]
  dt[, log_tna_l1 := shift(log_tna, 1L), by = crsp_fundno]
  
  # Flow (if not already present from Script 03)
  if (!("mtna_l1" %in% names(dt))) dt[, mtna_l1 := shift(mtna, 1L), by = crsp_fundno]
  if (!("flow" %in% names(dt))) {
    dt[, flow := fifelse(
      is.na(mtna_l1) | mtna_l1 <= 0 | is.na(mret),
      NA_real_,
      (mtna - mtna_l1 * (1 + mret)) / mtna_l1
    )]
  }
  if (!("flow_l1" %in% names(dt))) dt[, flow_l1 := shift(flow, 1L), by = crsp_fundno]
  
  # Family TNA (lagged) by family (mgmt_cd) x month
  dt[, family_tna_l1 := {
    if (all(is.na(mtna_l1))) NA_real_ else sum(mtna_l1, na.rm = TRUE)
  }, by = .(mgmt_cd, caldt)]
  dt[, log_familytna_l1 := log(pmax(family_tna_l1, 1e-6))]
  
  # 12m volatility of monthly returns (strict 12 months)
  dt[, r := as.numeric(mret)]
  dt[, `:=`(
    r_sum  = frollsum(r,   12L, align="right", na.rm=TRUE),
    r2_sum = frollsum(r^2, 12L, align="right", na.rm=TRUE),
    n12    = frollsum(as.integer(is.finite(r)), 12L, align="right", na.rm=TRUE)
  ), by = crsp_fundno]
  
  dt[, vol_12m := fifelse(
    n12 < 12L, NA_real_,
    sqrt(pmax((r2_sum - (r_sum^2)/n12) / (n12 - 1), 0))
  )]
  dt[, vol_12m_l1 := shift(vol_12m, 1L), by = crsp_fundno]
  dt[, c("r","r_sum","r2_sum","n12") := NULL]
  
  # -----------------------
  # B) Expense + turnover (from CRSP fund_summary*)
  # -----------------------
  close_wrds_on_exit <- FALSE
  if (is.null(wrds) && connect_if_null) {
    wrds <- DBI::dbConnect(
      RPostgres::Postgres(),
      host = "wrds-pgdata.wharton.upenn.edu",
      port = 9737,
      dbname = "wrds",
      sslmode = "require",
      user = wrds_user,
      options = "-c statement_timeout=0"
    )
    close_wrds_on_exit <- TRUE
  }
  if (!is.null(wrds) && close_wrds_on_exit) {
    on.exit(DBI::dbDisconnect(wrds), add = TRUE)
  }
  
  if (!is.null(wrds)) {
    
    # Prefer crsp.fund_summary2 if present; else crsp.fund_summary
    fs_table <- dbGetQuery(wrds, "
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema='crsp'
        AND table_name IN ('fund_summary2','fund_summary')
      ORDER BY CASE WHEN table_name='fund_summary2' THEN 1 ELSE 2 END
      LIMIT 1
    ")$table_name
    stopifnot(length(fs_table) == 1)
    
    fs_cols <- dbGetQuery(wrds, sprintf("
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema='crsp' AND table_name='%s'
    ", fs_table))$column_name
    
    fs_date_col <- if ("caldt" %in% fs_cols) "caldt"
    else if ("date" %in% fs_cols) "date"
    else if ("monthend" %in% fs_cols) "monthend"
    else if ("rptdt" %in% fs_cols) "rptdt"
    else NA_character_
    stopifnot(!is.na(fs_date_col))
    
    exp_col  <- c("exp_ratio","expratio","exp_rto","expense_ratio")[c("exp_ratio","expratio","exp_rto","expense_ratio") %in% fs_cols][1]
    turn_col <- c("turn_ratio","turnover","turn_rto","port_turnover")[c("turn_ratio","turnover","turn_rto","port_turnover") %in% fs_cols][1]
    stopifnot(!is.na(exp_col), !is.na(turn_col))
    
    start_date <- min(dt$caldt, na.rm = TRUE)
    end_date   <- max(dt$caldt, na.rm = TRUE)
    
    fundsum <- dbGetQuery(wrds, sprintf("
      SELECT crsp_fundno,
             %s AS caldt,
             %s AS exp_ratio,
             %s AS turn_ratio
      FROM crsp.%s
      WHERE %s BETWEEN '%s' AND '%s'
    ", fs_date_col, exp_col, turn_col, fs_table, fs_date_col,
                                        format(start_date, "%Y-%m-%d"), format(end_date, "%Y-%m-%d")))
    
    fundsum <- as.data.table(fundsum)
    fundsum[, caldt := as.Date(caldt)]
    fundsum[, crsp_fundno := as.numeric(crsp_fundno)]
    fundsum[, exp_ratio := as.numeric(exp_ratio)]
    fundsum[, turn_ratio := as.numeric(turn_ratio)]
    if (is.finite(mean(fundsum$exp_ratio, na.rm=TRUE)) && mean(fundsum$exp_ratio, na.rm=TRUE) > 1) fundsum[, exp_ratio := exp_ratio/100]
    if (is.finite(mean(fundsum$turn_ratio, na.rm=TRUE)) && mean(fundsum$turn_ratio, na.rm=TRUE) > 1) fundsum[, turn_ratio := turn_ratio/100]
    
    setkey(dt, crsp_fundno, caldt)
    setkey(fundsum, crsp_fundno, caldt)
    
    # as-of (carry last known) per fund
    fundsum_match <- fundsum[dt, on=.(crsp_fundno, caldt), roll=TRUE,
                             .(crsp_fundno, caldt, exp_ratio, turn_ratio)]
    
    dt[fundsum_match, `:=`(
      exp_ratio  = i.exp_ratio,
      turn_ratio = i.turn_ratio
    ), on=.(crsp_fundno, caldt)]
    
    dt[, exp_ratio_l1  := shift(exp_ratio,  1L), by=crsp_fundno]
    dt[, turn_ratio_l1 := shift(turn_ratio, 1L), by=crsp_fundno]
    
  } else {
    dt[, `:=`(
      exp_ratio = NA_real_, turn_ratio = NA_real_,
      exp_ratio_l1 = NA_real_, turn_ratio_l1 = NA_real_
    )]
  }
  
  # -----------------------
  # C) Optional loads (front/rear) from crspq.*
  # -----------------------
  if (include_loads && !is.null(wrds)) {
    
    front_exists <- dbGetQuery(wrds, "
      SELECT COUNT(*) AS n
      FROM information_schema.tables
      WHERE table_schema='crspq' AND table_name='front_load'
    ")$n[1] > 0
    
    rear_exists <- dbGetQuery(wrds, "
      SELECT COUNT(*) AS n
      FROM information_schema.tables
      WHERE table_schema='crspq' AND table_name='rear_load'
    ")$n[1] > 0
    
    start_date <- min(dt$caldt, na.rm = TRUE)
    end_date   <- max(dt$caldt, na.rm = TRUE)
    
    if (front_exists) {
      front <- dbGetQuery(wrds, sprintf("
        SELECT crsp_fundno, caldt, front_load
        FROM crspq.front_load
        WHERE caldt BETWEEN '%s' AND '%s'
      ", format(start_date, "%Y-%m-%d"), format(end_date, "%Y-%m-%d")))
      front <- as.data.table(front)
      front[, caldt := as.Date(caldt)]
      front[, crsp_fundno := as.numeric(crsp_fundno)]
      front[, front_load := as.numeric(front_load)]
      if (is.finite(mean(front$front_load, na.rm=TRUE)) && mean(front$front_load, na.rm=TRUE) > 1) front[, front_load := front_load/100]
      
      setkey(front, crsp_fundno, caldt)
      front_match <- front[dt, on=.(crsp_fundno, caldt), roll=TRUE, .(crsp_fundno, caldt, front_load)]
      dt[front_match, front_load := i.front_load, on=.(crsp_fundno, caldt)]
      dt[, front_load_l1 := shift(front_load, 1L), by=crsp_fundno]
    } else {
      dt[, `:=`(front_load=NA_real_, front_load_l1=NA_real_)]
    }
    
    if (rear_exists) {
      rear <- dbGetQuery(wrds, sprintf("
        SELECT crsp_fundno, caldt, rear_load
        FROM crspq.rear_load
        WHERE caldt BETWEEN '%s' AND '%s'
      ", format(start_date, "%Y-%m-%d"), format(end_date, "%Y-%m-%d")))
      rear <- as.data.table(rear)
      rear[, caldt := as.Date(caldt)]
      rear[, crsp_fundno := as.numeric(crsp_fundno)]
      rear[, rear_load := as.numeric(rear_load)]
      if (is.finite(mean(rear$rear_load, na.rm=TRUE)) && mean(rear$rear_load, na.rm=TRUE) > 1) rear[, rear_load := rear_load/100]
      
      setkey(rear, crsp_fundno, caldt)
      rear_match <- rear[dt, on=.(crsp_fundno, caldt), roll=TRUE, .(crsp_fundno, caldt, rear_load)]
      dt[rear_match, rear_load := i.rear_load, on=.(crsp_fundno, caldt)]
      dt[, rear_load_l1 := shift(rear_load, 1L), by=crsp_fundno]
    } else {
      dt[, `:=`(rear_load=NA_real_, rear_load_l1=NA_real_)]
    }
    
  } else {
    dt[, `:=`(
      front_load=NA_real_, front_load_l1=NA_real_,
      rear_load=NA_real_,  rear_load_l1=NA_real_
    )]
  }
  
  # -----------------------
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
assign("dt", dt, envir = .GlobalEnv)

cat("\n[04] Controls added.\n")
cat("[04] Rows:", nrow(dt), " Cols:", ncol(dt), "\n")

# -----------------------
# SAVE FINAL CSV (ONLY HERE)
# -----------------------
out_dir <- file.path(proj_root, "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

csv_file <- file.path(out_dir, "mf_with_names_2015_2026_equity_perf_controls.csv")
fwrite(dt, csv_file, sep = ",", quote = TRUE, na = "", bom = TRUE)

cat("[04] Saved CSV to:", csv_file, "\n")
stopifnot(file.exists(csv_file))
