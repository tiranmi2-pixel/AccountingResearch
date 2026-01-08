#==================== BUILD TSR MANUAL SEARCH LIST (EQUITY) + SAVE TO ONE CSV ====================

library(dplyr)
library(lubridate)
library(data.table)
library(DBI)

stopifnot(exists("mf_equity_us_domestic"), exists("wrds"))

#-----------------------
# 1) Output path (Desktop, non-OneDrive)
#-----------------------
proj_root <- normalizePath(
  file.path(
    Sys.getenv("USERPROFILE"),
    "Desktop", "Research Github", "AccountingResearch"
  ),
  winslash = "/",
  mustWork = TRUE
)

out_dir <- file.path(proj_root, "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "TSR Manual Search List - Equity.csv")
cat("Saving to:", out_csv, "\n")
stopifnot(dir.exists(out_dir))

#-----------------------
# 2) Choose ONE row per portfolio (crsp_portno)
#    Priority: website > retail > latest month
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
# 3) Search helpers
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

# Tracking columns for manual workflow
tsr_list[, tsr_found       := ""]
tsr_list[, tsr_url         := ""]
tsr_list[, tsr_report_date := ""]
tsr_list[, notes           := ""]

#-----------------------
# 3B) Add ALL CIKs using CRSP CIK map (crsp.crsp_cik_map)
#     WRDS columns: crsp_fundno, comp_cik, series_cik, contract_cik
#-----------------------

pad_cik10 <- function(x) {
  x <- trimws(as.character(x))
  x[x == ""] <- NA_character_
  x <- sub("\\.0+$", "", x)  # strip "1234.0" if present
  ifelse(
    !is.na(x) & grepl("^[0-9]+$", x),
    gsub(" ", "0", sprintf("%010s", x)),
    x
  )
}

# Pull CIK map from WRDS
cik_map <- data.table::as.data.table(DBI::dbGetQuery(wrds, "
  SELECT crsp_fundno, comp_cik, series_cik, contract_cik
  FROM crsp.crsp_cik_map
"))

# Standardize (keep as character, pad to 10 digits)
cik_map[, comp_cik     := pad_cik10(comp_cik)]
cik_map[, series_cik   := pad_cik10(series_cik)]
cik_map[, contract_cik := pad_cik10(contract_cik)]

# One row per fundno
data.table::setorder(cik_map, crsp_fundno)
cik_map <- unique(cik_map, by = "crsp_fundno")

# Ensure keys for update-join
data.table::setkey(tsr_list, crsp_fundno)
data.table::setkey(cik_map, crsp_fundno)

# Update-join: adds these columns to tsr_list (no merge suffixes)
tsr_list[
  cik_map,
  `:=`(
    comp_cik     = i.comp_cik,
    series_cik   = i.series_cik,
    contract_cik = i.contract_cik
  ),
  on = "crsp_fundno"
]

# Convenience "best" CIK to try first (series > contract > company)
tsr_list[, cik_best := fifelse(
  !is.na(series_cik) & nzchar(series_cik), series_cik,
  fifelse(
    !is.na(contract_cik) & nzchar(contract_cik), contract_cik,
    fifelse(
      !is.na(comp_cik) & nzchar(comp_cik), comp_cik,
      NA_character_
    )
  )
)]

tsr_list[, cik_best_num := suppressWarnings(as.integer(cik_best))]

# Coverage diagnostics (MUST be inside tsr_list[...] so columns are found)
cov_any <- tsr_list[, sum(!is.na(series_cik) | !is.na(contract_cik) | !is.na(comp_cik))]
cat("CIK coverage (any):", cov_any, "out of", nrow(tsr_list), "\n")

# Optional: show CIK columns present
# print(names(tsr_list)[grepl("cik", names(tsr_list))])

#-----------------------
# 4) Select + order columns (only what exists)
#-----------------------
keep_cols <- c(
  "crsp_portno",
  "crsp_fundno",
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
  "notes",
  # CIK fields (keep ALL)
  "series_cik",
  "contract_cik",
  "comp_cik",
  # Convenience best cik
  "cik_best",
  "cik_best_num"
)

keep_cols <- keep_cols[keep_cols %in% names(tsr_list)]
tsr_list  <- tsr_list[, ..keep_cols]

if ("website_clean" %in% names(tsr_list)) {
  setnames(tsr_list, "website_clean", "website")
}

#-----------------------
# 5) Final duplicate check + save
#-----------------------
stopifnot(tsr_list[, uniqueN(crsp_portno)] == nrow(tsr_list))

fwrite(tsr_list, out_csv, sep = ",", quote = TRUE, na = "", bom = TRUE)
message("Saved TSR manual search list CSV to: ", out_csv)
