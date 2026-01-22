library(data.table)

stopifnot(exists("dt_analysis"))
setDT(dt_analysis)

# ---------
# Output path 
# ---------
proj_root <- normalizePath(
  file.path(Sys.getenv("USERPROFILE"), "Desktop", "Research Github", "AccountingResearch"),
  winslash = "/", mustWork = TRUE
)

out_dir <- file.path(proj_root, "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_csv <- file.path(out_dir, "TSR Manual Search List - Equity.csv")


# Keep only columns relevant for the EDGAR downloader + manual search

keep_cols <- c(
  "crsp_portno","crsp_fundno","ticker","fund_name",
  "series_cik","contract_cik","comp_cik","cik_best",
  "website","city","state",
  "crsp_cl_grp","retail_fund","inst_fund",
  "caldt"
)

keep_cols <- keep_cols[keep_cols %in% names(dt_analysis)]
dt0 <- dt_analysis[, ..keep_cols]

# Standardize types/strings
dt0[, caldt := as.Date(caldt)]
dt0[, ticker := toupper(trimws(as.character(ticker)))]
dt0[, website_clean := fifelse(is.na(website), "", trimws(as.character(website)))]
dt0[, has_website := website_clean != ""]

dt0[, retail_flag := (toupper(trimws(as.character(retail_fund))) == "Y") &
      (toupper(trimws(as.character(inst_fund))) != "Y")]

# ---------
# Choose ONE row per crsp_portno:
# 1) has website
# 2) retail_flag
# 3) most recent caldt
# ---------
setorder(dt0, crsp_portno, -has_website, -retail_flag, -caldt)

tsr_list <- dt0[, .SD[1], by = crsp_portno]

# Drop helper cols
tsr_list[, c("website_clean","has_website","retail_flag") := NULL]

# Ensure crsp_portno is integer-like
tsr_list[, crsp_portno := as.integer(crsp_portno)]

# Save
fwrite(tsr_list, out_csv, sep = ",", quote = TRUE, na = "", bom = TRUE)

cat("Saved:", out_csv, "\n")
cat("Rows (unique crsp_portno):", nrow(tsr_list), "\n")
cat("Funds with any CIK:",
    tsr_list[
      (nzchar(series_cik) | nzchar(contract_cik) | nzchar(comp_cik)),
      .N
    ], "\n")
