#==================== STREAMLINED EQUITY FILTER + SAVE TO CSV (FULL FIXED) ====================
library(DBI)
library(dplyr)
library(lubridate)
library(data.table)
library(here)

stopifnot(exists("wrds"), exists("mf_with_names"))

#-----------------------
# 0) Output path (same folder as before)
#-----------------------
proj_root <- normalizePath(
  file.path(Sys.getenv("USERPROFILE"),
            "Desktop", "Research Github", "AccountingResearch"),
  winslash = "/", mustWork = TRUE
)

out_dir  <- file.path(proj_root, "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_file <- file.path(out_dir, "Raw Data- After fil for equity.csv")

cat("Project root:", proj_root, "\n")
cat("Saving to    :", out_file, "\n")
stopifnot(dir.exists(out_dir))

#-----------------------
# 1) Pick style table (prefer crsp_q_mutualfunds.fund_summary2)
#-----------------------
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

message("Using style table: ", style_schema, ".", style_table, " (ID = ", id_col, ")")

#-----------------------
# 2) Pull style fields for the sample range
#-----------------------
min_dt <- as.Date(min(mf_with_names$caldt, na.rm = TRUE))
max_dt <- as.Date(max(mf_with_names$caldt, na.rm = TRUE))

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

# keep only IDs that appear in your sample
ids_in_sample <- unique(as.numeric(mf_with_names[[id_col]]))
style_df <- style_df[style_id %in% ids_in_sample]

# dedupe (style_id, style_dt)
setkey(style_df, style_id, style_dt)
style_df <- unique(style_df, by = key(style_df))

#-----------------------
# 3) As-of (rolling) join to monthly panel  **BEST REMEDY**
#    Put j INSIDE the join so i.month_dt exists (avoids your error)
#-----------------------
mfk <- unique(data.table(
  style_id = as.numeric(mf_with_names[[id_col]]),
  month_dt = as.Date(mf_with_names$caldt)
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

#-----------------------
# 4) Update-join into mf_with_names
#-----------------------
mf_dt <- as.data.table(mf_with_names)
mf_dt[, style_id := as.numeric(get(id_col))]
setkey(mf_dt, style_id, caldt)

mf_dt[sty_at_month,
      `:=`(
        crsp_obj_cd     = i.crsp_obj_cd,
        policy          = i.policy,
        lipper_asset_cd = i.lipper_asset_cd
      ),
      on = .(style_id, caldt)
]

mf_dt[, style_id := NULL]

#-----------------------
# 5) Filter: US domestic equity
#-----------------------
mf_dt[, index_fund_flag := toupper(trimws(index_fund_flag))]
mf_dt[, et_flag         := toupper(trimws(et_flag))]
mf_dt[, crsp_obj_cd     := toupper(trimws(crsp_obj_cd))]
mf_dt[, lipper_asset_cd := toupper(trimws(lipper_asset_cd))]

mf_equity_us_domestic <- mf_dt[
  !is.na(crsp_obj_cd) &
    substr(crsp_obj_cd, 1, 2) == "ED" &
    lipper_asset_cd == "EQ" &
    (is.na(index_fund_flag) | index_fund_flag == "")
  # Optional ETF/ETN exclusion:
  # & (is.na(et_flag) | et_flag == "")
]

#-----------------------
# 6) Save to ONE CSV 
#-----------------------
fwrite(mf_equity_us_domestic, out_file, sep = ",", quote = TRUE, na = "", bom = TRUE)
message("Saved filtered equity CSV to: ", out_file)

#-----------------------
# 7) Diagnostics
#-----------------------
print(mf_equity_us_domestic[, .(
  n_rows          = .N,
  n_share_classes = uniqueN(crsp_fundno),
  n_portfolios    = uniqueN(crsp_portno),
  min_month       = min(caldt, na.rm = TRUE),
  max_month       = max(caldt, na.rm = TRUE)
)])
