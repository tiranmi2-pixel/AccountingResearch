# ===================== PANEL DESCRIPTIVE TABLE: PRE VS POST WITH DIFFS =====================
# PURPOSE:
#   Create one journal-style descriptive statistics table with panel structure
#   and pre/post columns for N, Mean, Median, plus difference columns.
#
# ASSUMPTION:
#   You have already run the full scripts set, so dt_24 exists in memory.
#
# OUTPUT:
#   1) desc_table_pre_post_with_diffs.csv
#   2) desc_table_pre_post_with_diffs.xlsx
#   3) desc_table_pre_post_with_diffs_raw
#   4) desc_table_pre_post_with_diffs_export
# ==============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(openxlsx)
})

# ------------------------------------------------------------------------------
# 0) USER OPTIONS
# ------------------------------------------------------------------------------
SAMPLE_OBJECT <- "dt_24"
USE_DARENDELI_FLOW <- TRUE
DIGITS <- 3

# ------------------------------------------------------------------------------
# 1) LOAD SAMPLE
# ------------------------------------------------------------------------------
stopifnot(exists(SAMPLE_OBJECT, envir = .GlobalEnv))
DT <- copy(get(SAMPLE_OBJECT, envir = .GlobalEnv))
setDT(DT)

if (!("caldt" %in% names(DT))) stop("Sample is missing caldt.")
DT[, caldt := as.Date(caldt)]

if (exists("proj_root", envir = .GlobalEnv)) {
  PROJ_ROOT <- get("proj_root", envir = .GlobalEnv)
} else {
  PROJ_ROOT <- getwd()
}
OUT_DIR <- file.path(PROJ_ROOT, "Descriptive Statistics")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# 2) HELPERS
# ------------------------------------------------------------------------------
yn_to_num <- function(x) {
  x <- toupper(trimws(as.character(x)))
  fifelse(x %in% c("Y", "YES", "1", "TRUE"), 1,
          fifelse(x %in% c("N", "NO", "0", "FALSE"), 0, NA_real_))
}

safe_mean <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  if (length(x) == 0 || all(is.na(x))) return(NA_real_)
  mean(x, na.rm = TRUE)
}

safe_median <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  if (length(x) == 0 || all(is.na(x))) return(NA_real_)
  median(x, na.rm = TRUE)
}

fmt_num <- function(x, digits = DIGITS) {
  ifelse(is.na(x), "", formatC(x, format = "f", digits = digits, big.mark = ","))
}

fmt_int <- function(x) {
  ifelse(is.na(x), "", formatC(as.integer(round(x)), format = "d", big.mark = ","))
}

# ------------------------------------------------------------------------------
# 3) DEFINE PRE / POST PERIOD
# ------------------------------------------------------------------------------
if (!("tsr_filingdate_first_mend" %in% names(DT))) {
  if ("tsr_filingdate_first" %in% names(DT)) {
    DT[, tsr_filingdate_first := as.Date(tsr_filingdate_first)]
    DT[, tsr_filingdate_first_mend := ceiling_date(tsr_filingdate_first, "month") - days(1)]
  } else {
    stop("Neither tsr_filingdate_first_mend nor tsr_filingdate_first exists.")
  }
}

DT[, post_tsr := as.numeric(!is.na(tsr_filingdate_first_mend) & caldt >= tsr_filingdate_first_mend)]
DT[, period := fifelse(post_tsr == 1, "Post", "Pre")]

# ------------------------------------------------------------------------------
# 4) BUILD / REFRESH SOME INDICATORS IF NEEDED
# ------------------------------------------------------------------------------
if (!("retail_ind" %in% names(DT)) && "retail_fund" %in% names(DT)) {
  DT[, retail_ind := yn_to_num(retail_fund)]
}
if (!("inst_ind" %in% names(DT)) && "inst_fund" %in% names(DT)) {
  DT[, inst_ind := yn_to_num(inst_fund)]
}
if (!("tsr_filing_observed" %in% names(DT)) && "tsr_filingdate_first" %in% names(DT)) {
  DT[, tsr_filing_observed := as.numeric(!is.na(tsr_filingdate_first))]
}
if (!("tsr_report_observed" %in% names(DT)) && "tsr_reportdate_first" %in% names(DT)) {
  DT[, tsr_report_observed := as.numeric(!is.na(tsr_reportdate_first))]
}

# ------------------------------------------------------------------------------
# 5) FLOW VARIABLE FOR REPORTING
# ------------------------------------------------------------------------------
# Default: use Darendeli-style return-adjusted flow in the descriptive table.
if (isTRUE(USE_DARENDELI_FLOW)) {
  req <- c("crsp_fundno", "mtna", "mret")
  miss <- setdiff(req, names(DT))
  if (length(miss) > 0) stop("Cannot build Darendeli flow. Missing: ", paste(miss, collapse = ", "))
  
  DT[, mtna := as.numeric(mtna)]
  DT[, mret := as.numeric(mret)]
  setorder(DT, crsp_fundno, caldt)
  DT[, mtna_l1_desc := shift(mtna, 1L), by = crsp_fundno]
  
  if (is.finite(mean(abs(DT$mret), na.rm = TRUE)) && mean(abs(DT$mret), na.rm = TRUE) > 1) {
    DT[, mret := mret / 100]
  }
  
  DT[, flow_desc := (mtna - mtna_l1_desc * (1 + mret)) / (mtna_l1_desc * (1 + mret))]
  DT[is.na(mtna_l1_desc) | mtna_l1_desc <= 0 | is.na(mret) | (1 + mret) <= 0, flow_desc := NA_real_]
} else {
  if (!("flow" %in% names(DT))) stop("flow not found and USE_DARENDELI_FLOW = FALSE.")
  DT[, flow_desc := as.numeric(flow)]
}

# ------------------------------------------------------------------------------
# 6) VARIABLE MAP
# ------------------------------------------------------------------------------
var_map <- data.table(
  panel = c(
    rep("Flow and performance", 9),
    rep("Fund characteristics", 8),
    rep("TSR indicators", 3)
  ),
  var = c(
    "flow_desc",
    "alpha_capm_12m", "alpha_capm_24m", "alpha_capm_36m",
    "alpha_carhart_12m", "alpha_carhart_24m", "alpha_carhart_36m",
    "fund_ret_12m", "fund_ret_60m",
    
    "age_years_l1",
    "log_tna_l1",
    "log_familytna_l1",
    "turn_ratio_l1",
    "mgmt_fee_l1",
    "exp_ratio_l1",
    "retail_ind",
    "inst_ind",
    
    "tsr_filing_observed",
    "tsr_report_observed",
    "post_tsr"
  ),
  label = c(
    "Flow",
    "CAPM alpha (12 months)",
    "CAPM alpha (24 months)",
    "CAPM alpha (36 months)",
    "Carhart alpha (12 months)",
    "Carhart alpha (24 months)",
    "Carhart alpha (36 months)",
    "Fund return (12 months)",
    "Fund return (60 months)",
    
    "Age (t-1)",
    "Log(TNA) (t-1)",
    "Log(FamilyTNA) (t-1)",
    "Turnover ratio (t-1)",
    "Management fee (t-1)",
    "Expense ratio (t-1)",
    "Retail indicator",
    "Institutional indicator",
    
    "TSR filing date observed",
    "TSR report date observed",
    "Post TSR month"
  ),
  scale = c(
    100,
    100, 100, 100,
    100, 100, 100,
    100, 100,
    
    1, 1, 1,
    100, 100, 100,
    100, 100,
    
    100, 100, 100
  )
)

var_map <- var_map[var %in% names(DT)]
if (nrow(var_map) == 0) stop("No requested variables found in the sample.")

# ------------------------------------------------------------------------------
# 7) BUILD RAW TABLE
# ------------------------------------------------------------------------------
summarize_pre_post_one <- function(DT, var, label, panel, scale = 1) {
  x_pre  <- suppressWarnings(as.numeric(DT[period == "Pre",  get(var)]))
  x_post <- suppressWarnings(as.numeric(DT[period == "Post", get(var)]))
  
  n_pre <- sum(!is.na(x_pre))
  n_post <- sum(!is.na(x_post))
  mean_pre <- safe_mean(x_pre) * scale
  mean_post <- safe_mean(x_post) * scale
  median_pre <- safe_median(x_pre) * scale
  median_post <- safe_median(x_post) * scale
  
  data.table(
    Panel = panel,
    Variable = label,
    N_Pre = n_pre,
    N_Post = n_post,
    N_Diff = n_post - n_pre,
    Mean_Pre = mean_pre,
    Mean_Post = mean_post,
    Mean_Diff = mean_post - mean_pre,
    Median_Pre = median_pre,
    Median_Post = median_post,
    Median_Diff = median_post - median_pre
  )
}

desc_table_pre_post_with_diffs_raw <- rbindlist(
  lapply(seq_len(nrow(var_map)), function(i) {
    summarize_pre_post_one(
      DT = DT,
      var = var_map$var[i],
      label = var_map$label[i],
      panel = var_map$panel[i],
      scale = var_map$scale[i]
    )
  }),
  fill = TRUE
)

# Make panel label appear only once per block in export
desc_table_pre_post_with_diffs_export <- copy(desc_table_pre_post_with_diffs_raw)
for (pp in unique(desc_table_pre_post_with_diffs_export$Panel)) {
  idx <- which(desc_table_pre_post_with_diffs_export$Panel == pp)
  if (length(idx) > 1) desc_table_pre_post_with_diffs_export$Panel[idx[-1]] <- ""
}

# ------------------------------------------------------------------------------
# 8) FORMAT FOR EXPORT
# ------------------------------------------------------------------------------
for (cc in c("N_Pre", "N_Post", "N_Diff")) {
  desc_table_pre_post_with_diffs_export[, (cc) := fmt_int(get(cc))]
}

for (cc in c("Mean_Pre", "Mean_Post", "Mean_Diff",
             "Median_Pre", "Median_Post", "Median_Diff")) {
  desc_table_pre_post_with_diffs_export[, (cc) := fmt_num(get(cc), digits = DIGITS)]
}

# ------------------------------------------------------------------------------
# 9) SAVE CSV
# ------------------------------------------------------------------------------
csv_file <- file.path(OUT_DIR, "desc_table_pre_post_with_diffs.csv")
fwrite(desc_table_pre_post_with_diffs_export, csv_file)

# ------------------------------------------------------------------------------
# 10) SAVE EXCEL
# ------------------------------------------------------------------------------
xlsx_file <- file.path(OUT_DIR, "desc_table_pre_post_with_diffs.xlsx")

wb <- createWorkbook()
addWorksheet(wb, "Table")

title_text <- paste0(
  "Table. Descriptive statistics by TSR period\n",
  "The table reports the number of non-missing observations, means, and medians for the pre-TSR and post-TSR periods in the final event-window sample, together with post-minus-pre differences.\n",
  if (USE_DARENDELI_FLOW) {
    "Flow is reported using the Darendeli return-adjusted measure."
  } else {
    "Flow is reported using the existing script-generated flow variable."
  }
)

writeData(wb, "Table", title_text, startRow = 1, startCol = 1)
writeData(wb, "Table", desc_table_pre_post_with_diffs_export, startRow = 5, startCol = 1, colNames = TRUE)

title_style <- createStyle(textDecoration = "bold", fontSize = 12, wrapText = TRUE)
head_style  <- createStyle(textDecoration = "bold", halign = "center", valign = "center")
panel_style <- createStyle(textDecoration = "bold")

addStyle(wb, "Table", title_style, rows = 1, cols = 1, gridExpand = TRUE)
addStyle(wb, "Table", head_style, rows = 5, cols = 1:ncol(desc_table_pre_post_with_diffs_export), gridExpand = TRUE)

panel_rows <- which(desc_table_pre_post_with_diffs_export$Panel != "") + 5
if (length(panel_rows) > 0) {
  addStyle(wb, "Table", panel_style, rows = panel_rows, cols = 1, gridExpand = TRUE)
}

setColWidths(wb, "Table", cols = 1:ncol(desc_table_pre_post_with_diffs_export), widths = "auto")
freezePane(wb, "Table", firstActiveRow = 6)

saveWorkbook(wb, xlsx_file, overwrite = TRUE)

# ------------------------------------------------------------------------------
# 11) PRINT PREVIEW
# ------------------------------------------------------------------------------
cat("\nSaved files:\n")
cat("  CSV : ", csv_file, "\n", sep = "")
cat("  XLSX: ", xlsx_file, "\n\n", sep = "")

print(desc_table_pre_post_with_diffs_export)

assign("desc_table_pre_post_with_diffs_raw", desc_table_pre_post_with_diffs_raw, envir = .GlobalEnv)
assign("desc_table_pre_post_with_diffs_export", desc_table_pre_post_with_diffs_export, envir = .GlobalEnv)