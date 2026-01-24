# ===================== 01_data_extraction.R =====================
# PURPOSE:
#  1) Connect WRDS with an auto-reconnect wrapper
#  2) Build base panel dt (IDs + contact + style fields) in memory
#  3) Pull FF factors + market index series objects for later scripts (ff, mkt_idx_df)
#  4) Auto-run Script 02

# =====================================================================
# ==== Libraries / basic setup ====
# =====================================================================
library(RPostgres)
library(dplyr)
library(DBI)
library(lubridate)
library(data.table)

# =====================================================================
# ==== WRDS auto-reconnect wrapper (same logic you use) ====
# =====================================================================
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

# Keep the original around so our wrapper can call it safely
.DBGetQuery_orig <- DBI::dbGetQuery

dbGetQuery <- function(conn, statement, ...) {
  # If WRDS dropped, just reconnect and update the global `wrds` handle
  if (is.null(conn) || !DBI::dbIsValid(conn)) {
    message("[WRDS] Connection not valid -> reconnecting...")
    conn <- wrds_reconnect()
    assign("wrds", conn, envir = .GlobalEnv)
  }
  
  # Run the query and catch errors so we can decide whether to retry
  out <- tryCatch(.DBGetQuery_orig(conn, statement, ...),
                  error = function(e) e)
  
  if (inherits(out, "error")) {
    # Heuristic: if the error smells like a broken connection, reconnect + retry once
    msg <- tolower(out$message)
    if (grepl("server closed|connection|terminat|ssl|timeout|broken pipe|reset|socket", msg)) {
      message("[WRDS] Query failed (likely disconnect) -> reconnect + retry once...")
      try(DBI::dbDisconnect(conn), silent = TRUE)
      conn <- wrds_reconnect()
      assign("wrds", conn, envir = .GlobalEnv)
      return(.DBGetQuery_orig(conn, statement, ...))
    }
    # Otherwise, it's a real error — surface it
    stop(out)
  }
  
  out
}

# =====================================================================
# ==== Connect + project root ====
# =====================================================================
wrds <- wrds_reconnect()

# This is where downstream scripts/results live on your machine
proj_root <- normalizePath(
  file.path(Sys.getenv("USERPROFILE"), "Desktop", "Research Github", "AccountingResearch"),
  winslash = "/", mustWork = TRUE
)

# =====================================================================
# ==== Step 1: Pull CRSP monthly return/TNA panel ====
# =====================================================================
mf_monthly_df <- dbGetQuery(wrds, "
  SELECT crsp_fundno, caldt, mret, mnav, mtna
  FROM crsp.monthly_tna_ret_nav
  WHERE caldt BETWEEN '2005-01-01' AND '2026-12-31'
")

# Make sure dates are real Date objects (helps with joins and filtering)
mf_monthly_df$caldt <- as.Date(mf_monthly_df$caldt)

# CRSP returns can sometimes show up as percent. If average abs return > 1,
# we treat it as percent and convert to decimal.
if (mean(abs(mf_monthly_df$mret), na.rm = TRUE) > 1) {
  mf_monthly_df$mret <- mf_monthly_df$mret / 100
}

# =====================================================================
# ==== Step 1b: Pull Fama-French monthly factors (saved for Script 03) ====
# =====================================================================
ff <- dbGetQuery(wrds, "
  SELECT
    dateff AS caldt,
    mktrf, smb, hml, umd, rf
  FROM ff.factors_monthly
  WHERE dateff BETWEEN '2015-01-01' AND '2026-12-31'
  ORDER BY dateff
")
ff$caldt <- as.Date(ff$caldt)

# Convert factor columns from percent to decimal (so math lines up with returns)
ff <- as.data.table(ff)
ff[, c("mktrf","smb","hml","umd","rf") :=
     lapply(.SD, function(x) as.numeric(x)/100),
   .SDcols = c("mktrf","smb","hml","umd","rf")]

# Convenience series: total market return = (market excess) + rf
mkt_idx_df <- ff[, .(caldt, mkt_ret = mktrf + rf)]

# =====================================================================
# ==== Step 2: Pull mapping + contact tables (static-ish attributes) ====
# =====================================================================
# portnomap is the key CRSP mapping layer: fundno -> portno and lots of identifiers/flags,
# but it’s time-valid (begdt/enddt), so we’ll filter by the month later.
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

# contact_info is also time-valid (chgdt/chgenddt), so we’ll apply a similar window filter
contact_df <- dbGetQuery(wrds, "
  SELECT crsp_fundno, chgdt, chgenddt, city, state, website
  FROM crsp.contact_info
")

# Normalize date columns so window filters behave correctly
contact_df$chgdt    <- as.Date(contact_df$chgdt)
contact_df$chgenddt <- as.Date(contact_df$chgenddt)

map_df$begdt <- as.Date(map_df$begdt)
map_df$enddt <- as.Date(map_df$enddt)
map_df$first_offer_dt <- as.Date(map_df$first_offer_dt)
map_df$mgr_dt <- as.Date(map_df$mgr_dt)
map_df$end_dt <- as.Date(map_df$end_dt)

# =====================================================================
# ==== Step 3: Merge monthly panel + portnomap (respect valid windows) ====
# =====================================================================
mf_with_names <- mf_monthly_df %>%
  left_join(map_df, by = "crsp_fundno") %>%
  # Keep only the portnomap row that’s valid for the month
  filter(caldt >= begdt & (is.na(enddt) | caldt <= enddt)) %>%
  group_by(crsp_fundno, caldt) %>%
  # If multiple rows match a month, keep the one with the latest begdt (most recent mapping)
  slice_max(begdt, n = 1, with_ties = FALSE) %>%
  ungroup()

# =====================================================================
# ==== Step 4: Attach contact info (respect valid windows) ====
# =====================================================================
mf_with_names <- mf_with_names %>%
  left_join(contact_df, by = "crsp_fundno") %>%
  # Keep only the contact row that’s valid for the month (or missing chgdt -> treat as always valid)
  filter(is.na(chgdt) | (caldt >= chgdt & (is.na(chgenddt) | caldt <= chgenddt))) %>%
  group_by(crsp_fundno, caldt) %>%
  # If multiple updates overlap, keep the latest chgdt (most recent contact record)
  slice_max(chgdt, n = 1, with_ties = FALSE) %>%
  ungroup()

# =====================================================================
# ==== Step 5: Add style fields (lipper_asset_cd + policy) from fund_summary2 ====
# =====================================================================
# We only pull style rows needed for our sample span (keeps the query smaller)
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

# Clean and key the style table for rolling joins
style_df <- as.data.table(style_df)
style_df[, crsp_portno := as.numeric(crsp_portno)]
style_df[, style_dt := as.Date(style_dt)]
style_df[, lipper_asset_cd := toupper(trimws(lipper_asset_cd))]
style_df[, policy := toupper(trimws(policy))]
setkey(style_df, crsp_portno, style_dt)

# If the source has duplicates for the same key, drop extras so joins are stable
style_df <- unique(style_df, by = key(style_df))

# Convert current panel into data.table so we can do a clean "as-of" (rolling) style merge
mf_tmp <- as.data.table(mf_with_names)
mf_tmp[, caldt := as.Date(caldt)]
mf_tmp[, crsp_portno := as.numeric(crsp_portno)]
setkey(mf_tmp, crsp_portno, caldt)

# Build a unique list of (portno, month) we need styles for
keys <- unique(mf_tmp[, .(crsp_portno, month_dt = caldt)])
setkey(keys, crsp_portno, month_dt)

# Rolling join:
# - Match exact style_dt if available
# - Otherwise roll back to the most recent prior style_dt for that portno (roll = Inf)
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

# Push the matched style fields back onto the main panel at (portno, caldt)
mf_tmp[sty_at_month,
       `:=`(lipper_asset_cd = i.lipper_asset_cd,
            policy          = i.policy,
            fiscal_yearend  = i.fiscal_yearend),
       on = .(crsp_portno, caldt)]

# =====================================================================
# ==== Step 5B: Add CIK fields (company / series / contract) via crsp_cik_map ====
# =====================================================================
# Helper to standardize CIKs:
# - trim whitespace
# - treat empty strings as NA
# - drop trailing ".0" if numeric got coerced to float somewhere
# - pad numeric CIKs to 10 digits
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

# Pull the mapping once (keyed by crsp_fundno)
cik_map <- data.table::as.data.table(DBI::dbGetQuery(wrds, "
  SELECT crsp_fundno, comp_cik, series_cik, contract_cik
  FROM crsp_q_mutualfunds.crsp_cik_map
"))

# Standardize types + apply padding cleanup to each CIK column
cik_map[, crsp_fundno := as.numeric(crsp_fundno)]
cik_map[, comp_cik     := pad_cik10(comp_cik)]
cik_map[, series_cik   := pad_cik10(series_cik)]
cik_map[, contract_cik := pad_cik10(contract_cik)]

# If there are duplicates per fundno, keep the row with the most filled-in CIKs
cik_map[, score := (!is.na(series_cik) & nzchar(series_cik)) +
          (!is.na(contract_cik) & nzchar(contract_cik)) +
          (!is.na(comp_cik) & nzchar(comp_cik))]
data.table::setorder(cik_map, crsp_fundno, -score)
cik_map <- unique(cik_map, by = "crsp_fundno")
cik_map[, score := NULL]

# Attach the CIK fields onto mf_tmp by fundno (in-place update)
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

# Pick a "best available" CIK:
# series first (best for mutual funds), then contract, then company, else NA
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

# =====================================================================
# ==== Convert back + enforce your preferred column order ====
# =====================================================================
mf_with_names <- as.data.frame(mf_tmp)

# Keep base “identifiers-left” order exactly as you had
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

# =====================================================================
# ==== Finalize dt + stash objects for downstream scripts ====
# =====================================================================
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

# =====================================================================
# ==== Auto-run Script 02 ====
# =====================================================================
source(file.path(proj_root, "Scripts", "02_equity_filter.R"))
