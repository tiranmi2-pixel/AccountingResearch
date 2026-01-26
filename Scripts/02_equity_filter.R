# ===================== 02_equity_filter.R =====================
# PURPOSE:
#  1) Add crsp_obj_cd (as-of) using existing style-table logic
#  2) Filter dt to US domestic equity funds
#  3) Auto-run Script 03

# =====================================================================
# ==== Libraries + basic sanity checks ====
# =====================================================================
library(DBI)
library(dplyr)
library(lubridate)
library(data.table)

# We expect Script 01 to have already created these in memory
stopifnot(exists("wrds"), exists("dt"), exists("proj_root"))

# Make sure we're working with a data.table (in-place updates below rely on that)
setDT(dt)

# We'll use this to preserve the original column order and only append new ones to the right
old_cols <- names(dt)  # preserve existing order

# =====================================================================
# ==== 1) Pick a "style" table to pull obj/policy/lipper from ====
# =====================================================================
# Goal here: choose the best available fund_summary table and decide whether we join by portno or fundno.
# We prefer crsp_q_mutualfunds.fund_summary2 if it has a usable ID column.
preferred <- dbGetQuery(wrds, "
  SELECT
    MAX(CASE WHEN column_name='crsp_portno' THEN 1 ELSE 0 END) AS has_portno,
    MAX(CASE WHEN column_name='crsp_fundno' THEN 1 ELSE 0 END) AS has_fundno
  FROM information_schema.columns
  WHERE table_schema='crsp_q_mutualfunds'
    AND table_name='fund_summary2'
    AND column_name IN ('crsp_portno','crsp_fundno')
")

if (nrow(preferred) == 1 && (preferred$has_portno == 1 || preferred$has_fundno == 1)) {
  # Nice — we can use the preferred schema/table
  style_schema <- "crsp_q_mutualfunds"
  style_table  <- "fund_summary2"
  id_col       <- if (preferred$has_portno == 1) "crsp_portno" else "crsp_fundno"
} else {
  # Fallback: scan for a usable fund_summary/fund_summary2 table in any crsp* schema
  style_candidates <- dbGetQuery(wrds, "
    WITH cols AS (
      SELECT
        table_schema,
        table_name,
        MAX(CASE WHEN column_name='crsp_portno' THEN 1 ELSE 0 END) AS has_portno,
        MAX(CASE WHEN column_name='crsp_fundno' THEN 1 ELSE 0 END) AS has_fundno,
        MAX(CASE WHEN column_name='caldt' THEN 1 ELSE 0 END) AS has_caldt,
        MAX(CASE WHEN column_name='crsp_obj_cd' THEN 1 ELSE 0 END) AS has_obj,
        MAX(CASE WHEN column_name='policy' THEN 1 ELSE 0 END) AS has_policy,
        MAX(CASE WHEN column_name='lipper_asset_cd' THEN 1 ELSE 0 END) AS has_lipper
      FROM information_schema.columns
      WHERE lower(table_name) IN ('fund_summary2','fund_summary')
        AND table_schema ILIKE 'crsp%'
      GROUP BY table_schema, table_name
    )
    SELECT *
    FROM cols
    WHERE has_caldt=1
      AND (has_obj=1 OR has_policy=1 OR has_lipper=1)
      AND (has_portno=1 OR has_fundno=1)
    ORDER BY
      CASE WHEN table_schema='crsp_q_mutualfunds' THEN 0 ELSE 1 END,
      CASE WHEN lower(table_name)='fund_summary2' THEN 0 ELSE 1 END,
      has_portno DESC,
      has_fundno DESC
    LIMIT 1
  ")
  if (nrow(style_candidates) == 0) stop("No usable fund_summary table found.")
  style_schema <- style_candidates$table_schema[1]
  style_table  <- style_candidates$table_name[1]
  id_col       <- if (style_candidates$has_portno[1] == 1) "crsp_portno" else "crsp_fundno"
}

message("[02] Using style table: ", style_schema, ".", style_table, " (ID = ", id_col, ")")

# =====================================================================
# ==== 2) Pull style fields for the dt date range ====
# =====================================================================
# Pull just the time window we need (keeps this lighter and avoids unnecessary rows)
min_dt <- as.Date(min(dt$caldt, na.rm = TRUE))
max_dt <- as.Date(max(dt$caldt, na.rm = TRUE))

style_sql <- sprintf("
  SELECT
    %s AS style_id,
    caldt AS style_dt,
    crsp_obj_cd,
    policy,
    lipper_asset_cd
  FROM %s.%s
  WHERE caldt BETWEEN '%s' AND '%s'
", id_col, style_schema, style_table, format(min_dt, "%Y-%m-%d"), format(max_dt, "%Y-%m-%d"))

style_df <- dbGetQuery(wrds, style_sql)
setDT(style_df)

# Standardize types/formatting so joins & comparisons behave consistently
style_df[, style_id := as.numeric(style_id)]
style_df[, style_dt := as.Date(style_dt)]
style_df[, crsp_obj_cd := toupper(trimws(crsp_obj_cd))]
style_df[, policy := toupper(trimws(policy))]
style_df[, lipper_asset_cd := toupper(trimws(lipper_asset_cd))]

# Keep only IDs actually present in our panel (no point carrying unrelated rows)
ids_in_sample <- unique(as.numeric(dt[[id_col]]))
style_df <- style_df[style_id %in% ids_in_sample]

# Dedupe on (style_id, style_dt) so the as-of join is stable/deterministic
setkey(style_df, style_id, style_dt)
style_df <- unique(style_df, by = key(style_df))

# =====================================================================
# ==== 3) As-of join style -> monthly panel (rolling "last known" value) ====
# =====================================================================
# We build a unique (style_id, month_dt) key list from dt, then roll style_dt backwards.
# This is the classic "as-of" / "last observation carried forward" pattern.
mfk <- unique(data.table(
  style_id = as.numeric(dt[[id_col]]),
  month_dt = as.Date(dt$caldt)
))
setkey(mfk, style_id, month_dt)

sty_at_month <- style_df[
  mfk,
  on   = .(style_id, style_dt = month_dt),
  roll = Inf,
  .(
    style_id        = i.style_id,
    caldt           = i.month_dt,
    crsp_obj_cd     = crsp_obj_cd,
    policy          = policy,
    lipper_asset_cd = lipper_asset_cd
  )
]

setkey(sty_at_month, style_id, caldt)
sty_at_month <- unique(sty_at_month, by = key(sty_at_month))

# =====================================================================
# ==== 4) Update-join into dt (append columns, don't reorder) ====
# =====================================================================
# We temporarily create a numeric style_id column in dt so the join key matches style_df/sty_at_month.
dt[, style_id := as.numeric(get(id_col))]
setkey(dt, style_id, caldt)

dt[sty_at_month,
   `:=`(
     crsp_obj_cd     = i.crsp_obj_cd,
     policy          = i.policy,
     lipper_asset_cd = i.lipper_asset_cd
   ),
   on = .(style_id, caldt)
]

# Clean up the temporary join key once we're done
dt[, style_id := NULL]

# Preserve existing order; any brand-new columns should end up on the far right
new_cols <- setdiff(names(dt), old_cols)
setcolorder(dt, c(old_cols, new_cols))

# =====================================================================
# ==== 5) Equity filtering logic (ETFs/ETNs -> Index -> CRSP ED -> Lipper check) ====
# =====================================================================

# Normalize flags/codes so comparisons like %in% and substr checks behave reliably
dt[, et_flag         := toupper(trimws(et_flag))]
dt[, index_fund_flag := toupper(trimws(index_fund_flag))]
dt[, crsp_obj_cd     := toupper(trimws(crsp_obj_cd))]
dt[, lipper_asset_cd := toupper(trimws(lipper_asset_cd))]

# ---------------------------------------------------------------------
# ==== 5.0 INFO-ONLY: Export filter drop summary for 2021-2025 window ====
# ---------------------------------------------------------------------
# Important: this uses a TEMP copy (dt_tmp) and does NOT change dt.
# We track counts as we apply each filter step, plus a quick Lipper-vs-CRSP-ED sanity check.
analysis_start <- as.Date("2021-01-01")
analysis_end   <- as.Date("2025-12-31")

dt_tmp <- copy(dt)[caldt >= analysis_start & caldt <= analysis_end]

# Helper: core counts at a step (unique funds, portfolios, rows)
count_step <- function(DT) {
  list(
    n_funds  = uniqueN(DT$crsp_fundno),
    n_portno = if ("crsp_portno" %in% names(DT)) uniqueN(DT$crsp_portno) else NA_integer_,
    n_rows   = nrow(DT)
  )
}

# Helper: among CRSP-ED funds, how often does Lipper agree/disagree/missing?
# This is computed per fund (crsp_fundno) so we don't over-count monthly rows.
count_lipper_vs_ed <- function(DT) {
  per_fund <- DT[, .(
    has_lipper = any(!is.na(lipper_asset_cd) & lipper_asset_cd != ""),
    any_eq     = any(lipper_asset_cd == "EQ", na.rm = TRUE),
    any_noneq  = any(!is.na(lipper_asset_cd) & lipper_asset_cd != "" & lipper_asset_cd != "EQ")
  ), by = crsp_fundno]
  
  list(
    ed_lipper_eq_funds      = per_fund[has_lipper == TRUE  & any_eq == TRUE & any_noneq == FALSE, .N],
    ed_lipper_non_eq_funds  = per_fund[has_lipper == TRUE  & any_noneq == TRUE, .N],
    ed_lipper_missing_funds = per_fund[has_lipper == FALSE, .N],
    ed_lipper_present_funds = per_fund[has_lipper == TRUE, .N]
  )
}

# Steps table (extra columns default to NA until we fill them)
steps <- data.table(
  step = character(),
  n_funds = integer(),
  n_portno = integer(),
  n_rows = integer(),
  ed_lipper_eq_funds = as.integer(NA),
  ed_lipper_non_eq_funds = as.integer(NA),
  ed_lipper_missing_funds = as.integer(NA),
  ed_lipper_present_funds = as.integer(NA)
)

# Step A: starting universe in 2021-2025
c0 <- count_step(dt_tmp)
steps <- rbind(steps, data.table(
  step="Start (all funds in dt, 2021-2025)",
  n_funds=c0$n_funds, n_portno=c0$n_portno, n_rows=c0$n_rows,
  ed_lipper_eq_funds=NA_integer_, ed_lipper_non_eq_funds=NA_integer_,
  ed_lipper_missing_funds=NA_integer_, ed_lipper_present_funds=NA_integer_
))

# Step B: exclude ETFs/ETNs using exact CRSP codes (F=ETF, N=ETN)
dt_tmp <- dt_tmp[!(et_flag %in% c("F","N"))]
c1 <- count_step(dt_tmp)
steps <- rbind(steps, data.table(
  step="After ETF/ETN exclusion (drop et_flag F/N)",
  n_funds=c1$n_funds, n_portno=c1$n_portno, n_rows=c1$n_rows,
  ed_lipper_eq_funds=NA_integer_, ed_lipper_non_eq_funds=NA_integer_,
  ed_lipper_missing_funds=NA_integer_, ed_lipper_present_funds=NA_integer_
))

# Step C: exclude index funds using exact CRSP codes (B/D/E)
dt_tmp <- dt_tmp[!(index_fund_flag %in% c("B","D","E"))]
c2 <- count_step(dt_tmp)
steps <- rbind(steps, data.table(
  step="After index exclusion (drop index_fund_flag B/D/E)",
  n_funds=c2$n_funds, n_portno=c2$n_portno, n_rows=c2$n_rows,
  ed_lipper_eq_funds=NA_integer_, ed_lipper_non_eq_funds=NA_integer_,
  ed_lipper_missing_funds=NA_integer_, ed_lipper_present_funds=NA_integer_
))

# Step D: keep Domestic Equity by CRSP objective code ED**
dt_tmp <- dt_tmp[!is.na(crsp_obj_cd) & substr(crsp_obj_cd, 1, 2) == "ED"]
c3 <- count_step(dt_tmp)

lip3 <- count_lipper_vs_ed(dt_tmp)

steps <- rbind(steps, data.table(
  step="After CRSP objective filter (crsp_obj_cd starts ED)",
  n_funds=c3$n_funds, n_portno=c3$n_portno, n_rows=c3$n_rows,
  ed_lipper_eq_funds=lip3$ed_lipper_eq_funds,
  ed_lipper_non_eq_funds=lip3$ed_lipper_non_eq_funds,
  ed_lipper_missing_funds=lip3$ed_lipper_missing_funds,
  ed_lipper_present_funds=lip3$ed_lipper_present_funds
))

# Step E: Lipper validation when available:
#     Keep EQ; if missing/blank keep; otherwise drop.
dt_tmp <- dt_tmp[is.na(lipper_asset_cd) | lipper_asset_cd == "" | lipper_asset_cd == "EQ"]
c4 <- count_step(dt_tmp)

# (optional) also show the “post-validation” breakdown (unmatched should be 0 by construction)
lip4 <- count_lipper_vs_ed(dt_tmp)

steps <- rbind(steps, data.table(
  step="After Lipper validation (EQ if available; keep if missing)",
  n_funds=c4$n_funds, n_portno=c4$n_portno, n_rows=c4$n_rows,
  ed_lipper_eq_funds=lip4$ed_lipper_eq_funds,
  ed_lipper_non_eq_funds=lip4$ed_lipper_non_eq_funds,
  ed_lipper_missing_funds=lip4$ed_lipper_missing_funds,
  ed_lipper_present_funds=lip4$ed_lipper_present_funds
))

# Add drop columns (fund counts) so the CSV is easy to scan
steps[, drop_funds_from_prev := shift(n_funds, 1L) - n_funds]
steps[, drop_funds_from_prev := fifelse(is.na(drop_funds_from_prev), 0L, drop_funds_from_prev)]
steps[, drop_funds_pct_prev := fifelse(shift(n_funds, 1L) > 0, 100 * drop_funds_from_prev / shift(n_funds, 1L), NA_real_)]

steps[, drop_funds_from_start := steps$n_funds[1] - n_funds]

base_n <- steps$n_funds[1]
if (isTRUE(base_n > 0)) {
  steps[, drop_funds_pct_start := 100 * drop_funds_from_start / base_n]
} else {
  steps[, drop_funds_pct_start := NA_real_]
}

# Extra interpretability columns (how often Lipper is present + mismatch rate among present)
steps[, `:=`(
  pct_lipper_present = fifelse(n_funds > 0, 100 * ed_lipper_present_funds / n_funds, NA_real_),
  pct_lipper_mismatch_present = fifelse(ed_lipper_present_funds > 0,
                                        100 * ed_lipper_non_eq_funds / ed_lipper_present_funds,
                                        NA_real_)
)]

# Save CSV (info-only)
out_dir <- file.path(proj_root, "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

summary_file <- file.path(out_dir, "filter_drop_summary_2021_2025_with_lipper_match.csv")
fwrite(steps, summary_file)

cat("[02] Saved filter drop summary to:", summary_file, "\n")


# =====================================================================
# ==== SNAPSHOT: save UNFILTERED universe with "final columns" ====
# =====================================================================
# Place this RIGHT BEFORE you start filtering dt (before 5.1).
# This will:
#   1) Copy the full, unfiltered dt (after style join)
#   2) Run 02b + 03 + 04 on the COPY (so it gets TSR + alphas + controls)
#   3) Save a single CSV with ALL columns, but NO equity/ETF/index filters applied

library(data.table)
stopifnot(exists("dt"), exists("proj_root"))

# 0) Make an unfiltered snapshot now (this is the key moment)
dt_unfilt <- copy(dt)

# 1) Temporarily swap global dt to the snapshot and run the enrichment pipeline on it
dt_saved_for_filters <- copy(dt)              # keep the working dt safe
assign("dt", dt_unfilt, envir = .GlobalEnv)   # downstream scripts read/write global dt

# Run the “final-column” builders on the unfiltered snapshot
source(file.path(proj_root, "Scripts", "02b_tsr_approx_distribution.R"))
source(file.path(proj_root, "Scripts", "03_alphas_market_adjusted_returns.R"))
source(file.path(proj_root, "Scripts", "04_controls_and_save.R"))

# Pull the enriched unfiltered dt back out
dt_unfilt_final <- copy(get("dt", envir = .GlobalEnv))

# 2) Restore the original dt so your equity filtering continues exactly as before
assign("dt", dt_saved_for_filters, envir = .GlobalEnv)
rm(dt_saved_for_filters)

# 3) Write the unfiltered “ALL FINAL COLS” CSV
out_dir <- file.path(proj_root, "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

unfilt_csv <- file.path(out_dir, "mf_unfiltered_all_final_columns.csv")
fwrite(dt_unfilt_final, unfilt_csv)

cat("[02] Saved UNFILTERED all-final-columns CSV to:", unfilt_csv, "\n")
cat("[02] Unfiltered rows:", nrow(dt_unfilt_final), " cols:", ncol(dt_unfilt_final), "\n")


# ---------------------------------------------------------------------
# ==== 5.1–5.4 Apply the same filters to the REAL dt (this changes dt) ====
# ---------------------------------------------------------------------

# 5.1 Exclude ETFs/ETNs (CRSP: F=ETF, N=ETN). Keep blank/NA.
dt <- dt[!(et_flag %in% c("F","N"))]

# 5.2 Exclude index funds (CRSP: B/D/E). Keep blank/NA.
dt <- dt[!(index_fund_flag %in% c("B","D","E"))]

# 5.3 Keep Domestic Equity by CRSP objective code (ED**)
dt <- dt[!is.na(crsp_obj_cd) & substr(crsp_obj_cd, 1, 2) == "ED"]

# 5.4 Lipper validation when available:
#     Keep EQ; if missing/blank keep (prioritize CRSP); otherwise drop.
dt <- dt[is.na(lipper_asset_cd) | lipper_asset_cd == "" | lipper_asset_cd == "EQ"]

# =====================================================================
# ==== Finalize + hand off to downstream scripts ====
# =====================================================================

# Keep dt sorted (helps rolling calcs later and makes debugging easier)
setorder(dt, crsp_fundno, caldt)

# Update the global dt so the next scripts pick up the filtered panel
assign("dt", dt, envir = .GlobalEnv)

cat("\n[02] Equity filter applied.\n")
cat("[02] Rows:", nrow(dt), " Cols:", ncol(dt), "\n")

# Auto-run Script 03 (and TSR distribution attach script)
source(file.path(proj_root, "Scripts", "02b_tsr_approx_distribution.R"))
source(file.path(proj_root, "Scripts", "03_alphas_market_adjusted_returns.R"))
