# ===================== 01_data_extraction.R =====================
# PURPOSE:
#  1) Connect WRDS with auto-reconnect wrapper
#  2) Build base panel dt (IDs + contact + style fields) in memory
#  3) Pull FF factors + market index series objects for later scripts (ff, mkt_idx_df)
#  4) Auto-run Script 02

# -----------------------
# Libraries
# -----------------------
library(RPostgres)
library(dplyr)
library(DBI)
library(lubridate)
library(data.table)

# -----------------------
# WRDS AUTO-RECONNECT (same logic you use)
# -----------------------
wrds_reconnect <- function() {
  DBI::dbConnect(
    RPostgres::Postgres(),
    host = "wrds-pgdata.wharton.upenn.edu",
    port = 9737,
    dbname = "wrds",
    sslmode = "require",
    user = "tiran",
    options = "-c statement_timeout=0"
  )
}

.DBGetQuery_orig <- DBI::dbGetQuery

dbGetQuery <- function(conn, statement, ...) {
  if (is.null(conn) || !DBI::dbIsValid(conn)) {
    message("[WRDS] Connection not valid -> reconnecting...")
    conn <- wrds_reconnect()
    assign("wrds", conn, envir = .GlobalEnv)
  }
  
  out <- tryCatch(.DBGetQuery_orig(conn, statement, ...),
                  error = function(e) e)
  
  if (inherits(out, "error")) {
    msg <- tolower(out$message)
    if (grepl("server closed|connection|terminat|ssl|timeout|broken pipe|reset|socket", msg)) {
      message("[WRDS] Query failed (likely disconnect) -> reconnect + retry once...")
      try(DBI::dbDisconnect(conn), silent = TRUE)
      conn <- wrds_reconnect()
      assign("wrds", conn, envir = .GlobalEnv)
      return(.DBGetQuery_orig(conn, statement, ...))
    }
    stop(out)
  }
  out
}

# -----------------------
# Connect + project root
# -----------------------
wrds <- wrds_reconnect()

proj_root <- normalizePath(
  file.path(Sys.getenv("USERPROFILE"), "Desktop", "Research Github", "AccountingResearch"),
  winslash = "/", mustWork = TRUE
)

# -----------------------
# Step 1: monthly panel
# -----------------------
mf_monthly_df <- dbGetQuery(wrds, "
  SELECT crsp_fundno, caldt, mret, mnav, mtna
  FROM crsp.monthly_tna_ret_nav
  WHERE caldt BETWEEN '2005-01-01' AND '2026-12-31'
")

mf_monthly_df$caldt <- as.Date(mf_monthly_df$caldt)
if (mean(abs(mf_monthly_df$mret), na.rm = TRUE) > 1) {
  mf_monthly_df$mret <- mf_monthly_df$mret / 100
}

# -----------------------
# Step 1b: FF monthly factors (saved for Script 03)
# -----------------------
ff <- dbGetQuery(wrds, "
  SELECT
    dateff AS caldt,
    mktrf, smb, hml, umd, rf
  FROM ff.factors_monthly
  WHERE dateff BETWEEN '2015-01-01' AND '2026-12-31'
  ORDER BY dateff
")
ff$caldt <- as.Date(ff$caldt)

ff <- as.data.table(ff)
ff[, c("mktrf","smb","hml","umd","rf") :=
     lapply(.SD, function(x) as.numeric(x)/100),
   .SDcols = c("mktrf","smb","hml","umd","rf")]

mkt_idx_df <- ff[, .(caldt, mkt_ret = mktrf + rf)]

# -----------------------
# Step 2: portnomap + contact_info
# -----------------------
map_df <- dbGetQuery(wrds, "
  SELECT crsp_fundno, crsp_portno, begdt, enddt,
         crsp_cl_grp, retail_fund, inst_fund,
         fund_name, ticker,
         index_fund_flag, et_flag, vau_fund, dead_flag,
         cusip8, ncusip, first_offer_dt,
         mgmt_name, mgmt_cd,
         mgr_name, mgr_dt,
         adv_name, open_to_inv,
         m_fund,
         end_dt, delist_cd, merge_fundno
  FROM crsp.portnomap
")

contact_df <- dbGetQuery(wrds, "
  SELECT crsp_fundno, chgdt, chgenddt, city, state, website
  FROM crsp.contact_info
")

contact_df$chgdt    <- as.Date(contact_df$chgdt)
contact_df$chgenddt <- as.Date(contact_df$chgenddt)

map_df$begdt <- as.Date(map_df$begdt)
map_df$enddt <- as.Date(map_df$enddt)
map_df$first_offer_dt <- as.Date(map_df$first_offer_dt)
map_df$mgr_dt <- as.Date(map_df$mgr_dt)
map_df$end_dt <- as.Date(map_df$end_dt)

# -----------------------
# Step 3: merge monthly + map (valid window, latest begdt)
# -----------------------
mf_with_names <- mf_monthly_df %>%
  left_join(map_df, by = "crsp_fundno") %>%
  filter(caldt >= begdt & (is.na(enddt) | caldt <= enddt)) %>%
  group_by(crsp_fundno, caldt) %>%
  slice_max(begdt, n = 1, with_ties = FALSE) %>%
  ungroup()

# -----------------------
# Step 4: attach contact (valid window, latest chgdt)
# -----------------------
mf_with_names <- mf_with_names %>%
  left_join(contact_df, by = "crsp_fundno") %>%
  filter(is.na(chgdt) | (caldt >= chgdt & (is.na(chgenddt) | caldt <= chgenddt))) %>%
  group_by(crsp_fundno, caldt) %>%
  slice_max(chgdt, n = 1, with_ties = FALSE) %>%
  ungroup()

# -----------------------
# Step 5: add style fields (lipper_asset_cd + policy) from fund_summary2
# -----------------------
min_dt <- as.Date(min(mf_with_names$caldt, na.rm = TRUE))
max_dt <- as.Date(max(mf_with_names$caldt, na.rm = TRUE))

style_df <- dbGetQuery(wrds, sprintf("
  SELECT
    crsp_portno,
    caldt AS style_dt,
    lipper_asset_cd,
    policy,
    fiscal_yearend
  FROM crsp_q_mutualfunds.fund_summary2
  WHERE caldt BETWEEN '%s' AND '%s'
", format(min_dt, "%Y-%m-%d"), format(max_dt, "%Y-%m-%d")))

style_df <- as.data.table(style_df)
style_df[, crsp_portno := as.numeric(crsp_portno)]
style_df[, style_dt := as.Date(style_dt)]
style_df[, lipper_asset_cd := toupper(trimws(lipper_asset_cd))]
style_df[, policy := toupper(trimws(policy))]
setkey(style_df, crsp_portno, style_dt)
style_df <- unique(style_df, by = key(style_df))

mf_tmp <- as.data.table(mf_with_names)
mf_tmp[, caldt := as.Date(caldt)]
mf_tmp[, crsp_portno := as.numeric(crsp_portno)]
setkey(mf_tmp, crsp_portno, caldt)

keys <- unique(mf_tmp[, .(crsp_portno, month_dt = caldt)])
setkey(keys, crsp_portno, month_dt)

sty_at_month <- style_df[
  keys,
  on   = .(crsp_portno, style_dt = month_dt),
  roll = Inf,
  .(crsp_portno = i.crsp_portno,
    caldt       = i.month_dt,
    lipper_asset_cd,
    policy,
    fiscal_yearend)
]

setkey(sty_at_month, crsp_portno, caldt)
sty_at_month <- unique(sty_at_month, by = key(sty_at_month))

mf_tmp[sty_at_month,
       `:=`(lipper_asset_cd = i.lipper_asset_cd,
            policy          = i.policy,
            fiscal_yearend  = i.fiscal_yearend),
       on = .(crsp_portno, caldt)]

# -----------------------
# Step 5B: add CIK fields (company / series / contract) from crsp_q_mutualfunds.crsp_cik_map
# Key: crsp_fundno
# -----------------------

pad_cik10 <- function(x) {
  x <- trimws(as.character(x))
  x[x == ""] <- NA_character_
  x <- sub("\\.0+$", "", x)  # strip "1234.0" if present
  ifelse(
    !is.na(x) & grepl("^[0-9]+$", x),
    sprintf("%010d", as.integer(x)),
    x
  )
}

cik_map <- data.table::as.data.table(DBI::dbGetQuery(wrds, "
  SELECT crsp_fundno, comp_cik, series_cik, contract_cik
  FROM crsp_q_mutualfunds.crsp_cik_map
"))

# Standardize types + pad
cik_map[, crsp_fundno := as.numeric(crsp_fundno)]
cik_map[, comp_cik     := pad_cik10(comp_cik)]
cik_map[, series_cik   := pad_cik10(series_cik)]
cik_map[, contract_cik := pad_cik10(contract_cik)]

# One row per fundno (if duplicates exist, keep the most informative)
cik_map[, score := (!is.na(series_cik) & nzchar(series_cik)) +
          (!is.na(contract_cik) & nzchar(contract_cik)) +
          (!is.na(comp_cik) & nzchar(comp_cik))]
data.table::setorder(cik_map, crsp_fundno, -score)
cik_map <- unique(cik_map, by = "crsp_fundno")
cik_map[, score := NULL]

# Attach to mf_tmp (data.table) by fundno
data.table::setkey(mf_tmp, crsp_fundno)
data.table::setkey(cik_map, crsp_fundno)

mf_tmp[
  cik_map,
  `:=`(
    comp_cik     = i.comp_cik,
    series_cik   = i.series_cik,
    contract_cik = i.contract_cik
  )
]


mf_tmp[, cik_best := data.table::fifelse(
  !is.na(series_cik) & nzchar(series_cik), series_cik,
  data.table::fifelse(
    !is.na(contract_cik) & nzchar(contract_cik), contract_cik,
    data.table::fifelse(
      !is.na(comp_cik) & nzchar(comp_cik), comp_cik,
      NA_character_
    )
  )
)]



mf_with_names <- as.data.frame(mf_tmp)

# Keep  base “identifiers-left” order exactly as you had
mf_with_names <- mf_with_names %>%
  select(
    crsp_fundno, crsp_portno, ticker, fund_name,
    comp_cik, series_cik, contract_cik, cik_best,
    lipper_asset_cd, policy,
    city, state, website,
    crsp_cl_grp, retail_fund, inst_fund,
    caldt, mret, mnav, mtna,
    begdt, enddt,
    cusip8, ncusip, first_offer_dt,
    mgmt_name, mgmt_cd,
    mgr_name, mgr_dt,
    adv_name, open_to_inv,
    m_fund,
    index_fund_flag, vau_fund, et_flag,
    end_dt, dead_flag, delist_cd, merge_fundno
  )

dt <- as.data.table(mf_with_names)
dt[, caldt := as.Date(caldt)]
setorder(dt, crsp_fundno, caldt)

# Save objects into global env for downstream scripts
assign("dt", dt, envir = .GlobalEnv)
assign("ff", ff, envir = .GlobalEnv)
assign("mkt_idx_df", mkt_idx_df, envir = .GlobalEnv)
assign("proj_root", proj_root, envir = .GlobalEnv)
assign("wrds", wrds, envir = .GlobalEnv)

cat("\n[01] Base dt created in memory.\n")
cat("[01] Rows:", nrow(dt), " Cols:", ncol(dt), "\n")

# -----------------------
# Auto-run Script 02
# -----------------------
source(file.path(proj_root, "Scripts", "02_equity_filter.R"))
