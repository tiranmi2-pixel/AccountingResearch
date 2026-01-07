#==================== BUILD TSR MANUAL SEARCH LIST (EQUITY) + SAVE TO ONE CSV ====================
library(dplyr)
library(lubridate)
library(data.table)

stopifnot(exists("mf_equity_us_domestic"))

#-----------------------
# 1) Output path (same folder as before)
#-----------------------
out_dir  <- file.path(getwd(), "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "TSR Manual Search List - Equity.csv")

#-----------------------
# 2) Convert to data.table + choose ONE row per portfolio (crsp_portno)
#    Priority for representative row:
#      1) has website
#      2) retail share class (retail=Y & inst!=Y)
#      3) most recent month (latest caldt)
#-----------------------
dt <- as.data.table(mf_equity_us_domestic)

dt[, caldt := as.Date(caldt)]

dt[, website_clean := fifelse(is.na(website), "", trimws(website))]
dt[, has_website   := website_clean != ""]

dt[, retail_flag := toupper(trimws(fifelse(is.na(retail_fund), "", retail_fund)))]
dt[, inst_flag   := toupper(trimws(fifelse(is.na(inst_fund),   "", inst_fund)))]
dt[, prefer_retail := (retail_flag == "Y" & inst_flag != "Y")]

setorder(dt, crsp_portno, -has_website, -prefer_retail, -caldt)

tsr_list <- dt[, .SD[1], by = crsp_portno]

#-----------------------
# 3) Add helper fields for searching
#-----------------------
tsr_list[, website_domain := {
  w <- tolower(website_clean)
  w <- sub("^https?://", "", w)
  w <- sub("^www\\.", "", w)
  w <- sub("/.*$", "", w)
  fifelse(w == "", NA_character_, w)
}]

tsr_list[, search_hint := fifelse(
  !is.na(website_domain),
  paste0('"', fund_name, '" "tailored shareholder report" site:', website_domain),
  paste0('"', fund_name, '" "tailored shareholder report"')
)]

# Tracking columns for your manual workflow
tsr_list[, tsr_found       := ""]
tsr_list[, tsr_url         := ""]
tsr_list[, tsr_report_date := ""]
tsr_list[, notes           := ""]

#-----------------------
# 4) Select + order columns (keeps only columns that exist)
#-----------------------
keep_cols <- c(
  "crsp_portno",
  "crsp_fundno",          # representative share class picked by priority rules
  "fund_name",
  "ticker",
  "website_clean",
  "website_domain",
  "city",
  "state",
  "cusip8",
  "ncusip",
  "mgmt_name",
  "adv_name",
  "open_to_inv",
  "first_offer_dt",
  "retail_flag",
  "inst_flag",
  "crsp_obj_cd",
  "lipper_asset_cd",
  "policy",
  "search_hint",
  "tsr_found",
  "tsr_url",
  "tsr_report_date",
  "notes"
)

keep_cols <- keep_cols[keep_cols %in% names(tsr_list)]
tsr_list  <- tsr_list[, ..keep_cols]

# Rename website column back to "website" for readability
if ("website_clean" %in% names(tsr_list)) {
  setnames(tsr_list, "website_clean", "website")
}

#-----------------------
# 5) Final duplicate check + save to ONE CSV
#-----------------------
stopifnot(tsr_list[, uniqueN(crsp_portno)] == nrow(tsr_list))

fwrite(tsr_list, out_csv, sep = ",", quote = TRUE, na = "", bom = TRUE)
message("Saved TSR manual search list CSV to: ", out_csv)
