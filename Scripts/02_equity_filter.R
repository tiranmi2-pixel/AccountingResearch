# ===================== 02_equity_filter.R =====================
# PURPOSE:
#  1) Add crsp_obj_cd (as-of) using your existing style-table logic
#  2) Filter dt to US domestic equity funds
#  3) Auto-run Script 03
#
# NOTE:
#  - No CSV I/O. dt stays in memory.
#  - Existing column order is preserved; only new columns are appended to the right.

library(DBI)
library(dplyr)
library(lubridate)
library(data.table)

stopifnot(exists("wrds"), exists("dt"), exists("proj_root"))

setDT(dt)
old_cols <- names(dt)  # preserve existing order

# -----------------------
# 1) Pick style table (prefer crsp_q_mutualfunds.fund_summary2)
# -----------------------
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
  style_schema <- "crsp_q_mutualfunds"
  style_table  <- "fund_summary2"
  id_col       <- if (preferred$has_portno == 1) "crsp_portno" else "crsp_fundno"
} else {
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

# -----------------------
# 2) Pull style fields
# -----------------------
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
style_df[, style_id := as.numeric(style_id)]
style_df[, style_dt := as.Date(style_dt)]
style_df[, crsp_obj_cd := toupper(trimws(crsp_obj_cd))]
style_df[, policy := toupper(trimws(policy))]
style_df[, lipper_asset_cd := toupper(trimws(lipper_asset_cd))]

# keep only IDs in sample
ids_in_sample <- unique(as.numeric(dt[[id_col]]))
style_df <- style_df[style_id %in% ids_in_sample]

# dedupe
setkey(style_df, style_id, style_dt)
style_df <- unique(style_df, by = key(style_df))

# -----------------------
# 3) As-of join to monthly panel (same logic; fixes i.month_dt scope)
# -----------------------
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

# -----------------------
# 4) Update-join into dt (APPENDS cols; no reorder)
# -----------------------
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

dt[, style_id := NULL]

# Preserve existing order; append new cols at right
new_cols <- setdiff(names(dt), old_cols)
setcolorder(dt, c(old_cols, new_cols))

# -----------------------
# 5) Filter: US domestic equity (your logic)
# -----------------------
dt[, index_fund_flag := toupper(trimws(index_fund_flag))]
dt[, et_flag         := toupper(trimws(et_flag))]
dt[, crsp_obj_cd     := toupper(trimws(crsp_obj_cd))]
dt[, lipper_asset_cd := toupper(trimws(lipper_asset_cd))]

dt <- dt[
  !is.na(crsp_obj_cd) &
    substr(crsp_obj_cd, 1, 2) == "ED" &
    lipper_asset_cd == "EQ" &
    (is.na(index_fund_flag) | index_fund_flag == "")
  # Optional ETF/ETN exclusion:
  # & (is.na(et_flag) | et_flag == "")
]

# Keep dt sorted
setorder(dt, crsp_fundno, caldt)

assign("dt", dt, envir = .GlobalEnv)

cat("\n[02] Equity filter applied.\n")
cat("[02] Rows:", nrow(dt), " Cols:", ncol(dt), "\n")

# Auto-run Script 03
source(file.path(proj_root, "Scripts", "03_alphas_market_adjusted_returns.R"))
