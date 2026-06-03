# ===================== TABLE 2: CORE DESCRIPTIVE STATISTICS =====================
# PURPOSE:
#   Build a journal-style descriptive statistics table for the final sample already
#   created by your full pipeline (default: dt_24).
#
# OUTPUT:
#   1) desc_table_core.csv
#   2) desc_table_core.xlsx
#   3) desc_table_core (in memory)
#
# DEFAULT SAMPLE:
#   dt_24  (final event-window sample kept in memory by Script 06)
#
# DEFAULT WITHIN-FE SD:
#   share-class FE + month FE  ->  crsp_fundno + ym
#
# NOTES:
#   - This script is designed to run AFTER your full scripts set has already run.
#   - It does NOT recompute alphas or controls.
#   - It uses Darendeli-style flow only if you choose to construct it below.
#     Otherwise it will use the existing flow variable in dt_24.
#   - Edit USE_DARENDELI_FLOW if needed.
# ==============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(fixest)
  library(openxlsx)
})

# ------------------------------------------------------------------------------
# 0) USER OPTIONS
# ------------------------------------------------------------------------------
SAMPLE_OBJECT <- "dt_24"          # final sample already in memory
USE_DARENDELI_FLOW <- TRUE        # TRUE = build Darendeli flow for table
FLOW_VAR_NAME <- "flow_desc"      # name to use in the table script
DIGITS <- 3                       # rounding for exported numbers

# Within-FE SD: choose FE structure aligned with your main regressions
FE_ID_VAR   <- "crsp_fundno"      # alternatives: "crsp_portno"
FE_TIME_VAR <- "ym"

# ------------------------------------------------------------------------------
# 1) LOAD SAMPLE
# ------------------------------------------------------------------------------
stopifnot(exists(SAMPLE_OBJECT, envir = .GlobalEnv))
DT <- copy(get(SAMPLE_OBJECT, envir = .GlobalEnv))
setDT(DT)

if (!("caldt" %in% names(DT))) stop("Sample is missing caldt.")
DT[, caldt := as.Date(caldt)]

# Project root / output folder
if (exists("proj_root", envir = .GlobalEnv)) {
  PROJ_ROOT <- get("proj_root", envir = .GlobalEnv)
} else {
  PROJ_ROOT <- getwd()
}
OUT_DIR <- file.path(PROJ_ROOT, "Descriptive Statistics")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# 2) BASIC HELPER VARIABLES
# ------------------------------------------------------------------------------
# Month fixed effect
if (!("ym" %in% names(DT))) {
  DT[, ym := format(caldt, "%Y-%m")]
}

# Clean Y/N style indicators into 0/1 numeric
yn_to_num <- function(x) {
  x <- toupper(trimws(as.character(x)))
  fifelse(x %in% c("Y", "YES", "1", "TRUE"), 1,
          fifelse(x %in% c("N", "NO", "0", "FALSE"), 0, NA_real_))
}

if (!("retail_ind" %in% names(DT))) {
  if ("retail_fund" %in% names(DT)) DT[, retail_ind := yn_to_num(retail_fund)]
}
if (!("inst_ind" %in% names(DT))) {
  if ("inst_fund" %in% names(DT)) DT[, inst_ind := yn_to_num(inst_fund)]
}

# TSR availability indicators
if (!("tsr_filing_observed" %in% names(DT)) && "tsr_filingdate_first" %in% names(DT)) {
  DT[, tsr_filing_observed := as.numeric(!is.na(tsr_filingdate_first))]
}
if (!("tsr_report_observed" %in% names(DT)) && "tsr_reportdate_first" %in% names(DT)) {
  DT[, tsr_report_observed := as.numeric(!is.na(tsr_reportdate_first))]
}

# Portfolio-specific post indicator
if (!("tsr_filingdate_first_mend" %in% names(DT)) && "tsr_filingdate_first" %in% names(DT)) {
  DT[, tsr_filingdate_first := as.Date(tsr_filingdate_first)]
  DT[, tsr_filingdate_first_mend := ceiling_date(tsr_filingdate_first, "month") - days(1)]
}
if (!("post_fund" %in% names(DT)) && "tsr_filingdate_first_mend" %in% names(DT)) {
  DT[, post_fund := as.numeric(!is.na(tsr_filingdate_first_mend) & caldt >= tsr_filingdate_first_mend)]
}

# ------------------------------------------------------------------------------
# 3) FLOW VARIABLE FOR DESCRIPTIVES
# ------------------------------------------------------------------------------
# Option A: Darendeli-style return-adjusted flow
if (isTRUE(USE_DARENDELI_FLOW)) {
  req_flow <- c("mtna", "mret", "crsp_fundno")
  miss_flow <- setdiff(req_flow, names(DT))
  if (length(miss_flow) > 0) {
    stop("Cannot build Darendeli flow. Missing columns: ", paste(miss_flow, collapse = ", "))
  }
  
  DT[, mtna := as.numeric(mtna)]
  DT[, mret := as.numeric(mret)]
  DT[, mtna_l1_desc := shift(mtna, 1L), by = crsp_fundno]
  
  # Guard if returns still look like percent
  if (is.finite(mean(abs(DT$mret), na.rm = TRUE)) && mean(abs(DT$mret), na.rm = TRUE) > 1) {
    DT[, mret := mret / 100]
  }
  
  DT[, (FLOW_VAR_NAME) := (mtna - mtna_l1_desc * (1 + mret)) / (mtna_l1_desc * (1 + mret))]
  DT[is.na(mtna_l1_desc) | mtna_l1_desc <= 0 | is.na(mret) | (1 + mret) <= 0, (FLOW_VAR_NAME) := NA_real_]
} else {
  if (!("flow" %in% names(DT))) stop("flow not found and USE_DARENDELI_FLOW = FALSE.")
  DT[, (FLOW_VAR_NAME) := as.numeric(flow)]
}

# ------------------------------------------------------------------------------
# 4) VARIABLE MAP FOR THE TABLE
# ------------------------------------------------------------------------------
# scale = 100 means reported in percentage points / percentages
var_map <- data.table(
  panel = c(
    rep("Flow and performance", 9),
    rep("Fund characteristics", 8),
    rep("TSR indicators", 3)
  ),
  var = c(
    FLOW_VAR_NAME,
    "alpha_capm_12m", "alpha_capm_24m", "alpha_capm_36m",
    "alpha_carhart_12m", "alpha_carhart_24m", "alpha_carhart_36m",
    "fund_ret_12m", "fund_ret_60m",
    
    "age_years_l1", "log_tna_l1", "log_familytna_l1",
    "turn_ratio_l1", "mgmt_fee_l1", "exp_ratio_l1",
    "retail_ind", "inst_ind",
    
    "tsr_filing_observed", "tsr_report_observed", "post_fund"
  ),
  label = c(
    "Flow",
    "CAPM alpha (12 months)", "CAPM alpha (24 months)", "CAPM alpha (36 months)",
    "Carhart alpha (12 months)", "Carhart alpha (24 months)", "Carhart alpha (36 months)",
    "Fund return (12 months)", "Fund return (60 months)",
    
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
    100, 100, 100, 100, 100, 100, 100, 100, 100,
    1, 1, 1, 100, 100, 100, 100, 100,
    100, 100, 100
  ),
  within_fe = c(
    TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
    TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE,
    FALSE, FALSE, TRUE
  )
)

# Keep only variables actually present
var_map <- var_map[var %in% names(DT)]

if (nrow(var_map) == 0) stop("No requested variables found in sample.")

# ------------------------------------------------------------------------------
# 5) WITHIN-FE SD HELPER
# ------------------------------------------------------------------------------
calc_within_fe_sd <- function(DT, yvar, fe_id = FE_ID_VAR, fe_time = FE_TIME_VAR) {
  stopifnot(yvar %in% names(DT))
  if (!(fe_id %in% names(DT)) || !(fe_time %in% names(DT))) return(NA_real_)
  
  sub <- DT[!is.na(get(yvar)) & !is.na(get(fe_id)) & !is.na(get(fe_time))]
  if (nrow(sub) < 2) return(NA_real_)
  
  fml <- as.formula(paste0(yvar, " ~ 1 | ", fe_id, " + ", fe_time))
  fit <- tryCatch(
    fixest::feols(fml, data = sub, warn = FALSE, notes = FALSE),
    error = function(e) NULL
  )
  if (is.null(fit)) return(NA_real_)
  
  sd(resid(fit), na.rm = TRUE)
}

# ------------------------------------------------------------------------------
# 6) BUILD RAW SUMMARY TABLE
# ------------------------------------------------------------------------------
summarize_one <- function(DT, var, label, scale = 1, within_fe = TRUE) {
  x <- DT[[var]]
  x_num <- suppressWarnings(as.numeric(x))
  x_num[is.infinite(x_num)] <- NA_real_
  
  N_nonmiss <- sum(!is.na(x_num))
  if (N_nonmiss == 0) {
    return(data.table(
      Variable = label,
      N = 0L,
      Mean = NA_real_,
      SD = NA_real_,
      P25 = NA_real_,
      Median = NA_real_,
      P75 = NA_real_,
      `Within-FE SD` = NA_real_
    ))
  }
  
  wsd <- if (within_fe) calc_within_fe_sd(DT, var) else NA_real_
  
  data.table(
    Variable = label,
    N = N_nonmiss,
    Mean = mean(x_num, na.rm = TRUE) * scale,
    SD = sd(x_num, na.rm = TRUE) * scale,
    P25 = as.numeric(quantile(x_num, 0.25, na.rm = TRUE, type = 7)) * scale,
    Median = as.numeric(quantile(x_num, 0.50, na.rm = TRUE, type = 7)) * scale,
    P75 = as.numeric(quantile(x_num, 0.75, na.rm = TRUE, type = 7)) * scale,
    `Within-FE SD` = wsd * scale
  )
}

desc_table_core <- rbindlist(
  lapply(seq_len(nrow(var_map)), function(i) {
    out <- summarize_one(
      DT = DT,
      var = var_map$var[i],
      label = var_map$label[i],
      scale = var_map$scale[i],
      within_fe = var_map$within_fe[i]
    )
    out[, Panel := var_map$panel[i]]
    out
  }),
  fill = TRUE
)

setcolorder(desc_table_core,
            c("Panel", "Variable", "N", "Mean", "SD", "P25", "Median", "P75", "Within-FE SD"))

# ------------------------------------------------------------------------------
# 7) FORMAT FOR JOURNAL-TYPE OUTPUT
# ------------------------------------------------------------------------------
fmt_num <- function(x, digits = DIGITS) {
  ifelse(is.na(x), "",
         formatC(x, format = "f", digits = digits, big.mark = ","))
}

desc_table_export <- copy(desc_table_core)

# Format N with commas
desc_table_export[, N := formatC(N, format = "d", big.mark = ",")]

# Format statistic columns
stat_cols <- c("Mean", "SD", "P25", "Median", "P75", "Within-FE SD")
for (cc in stat_cols) {
  desc_table_export[, (cc) := fmt_num(get(cc), digits = DIGITS)]
}

# Blank out repeated panel labels for cleaner Word look
for (pp in unique(desc_table_export$Panel)) {
  idx <- which(desc_table_export$Panel == pp)
  if (length(idx) > 1) desc_table_export$Panel[idx[-1]] <- ""
}

# ------------------------------------------------------------------------------
# 8) SAVE OUTPUTS
# ------------------------------------------------------------------------------
csv_file  <- file.path(OUT_DIR, "desc_table_core.csv")
xlsx_file <- file.path(OUT_DIR, "desc_table_core.xlsx")

fwrite(desc_table_export, csv_file)

wb <- createWorkbook()
addWorksheet(wb, "Table2_Descriptives")

title_text <- paste0(
  "Table 2. Descriptive statistics\n",
  "Sample: ", SAMPLE_OBJECT, "\n",
  "Within-FE SD uses fixed effects for ", FE_ID_VAR, " and ", FE_TIME_VAR, ".\n",
  if (USE_DARENDELI_FLOW) {
    "Flow row uses Darendeli-style return-adjusted flow for descriptive reporting."
  } else {
    "Flow row uses the existing flow variable stored in the sample."
  }
)

writeData(wb, "Table2_Descriptives", title_text, startRow = 1, startCol = 1)
writeData(wb, "Table2_Descriptives", desc_table_export, startRow = 5, startCol = 1, colNames = TRUE)

# Basic styling
hs <- createStyle(textDecoration = "bold", halign = "center", valign = "center")
title_style <- createStyle(textDecoration = "bold", fontSize = 12, wrapText = TRUE)
panel_style <- createStyle(textDecoration = "bold")

addStyle(wb, "Table2_Descriptives", title_style, rows = 1, cols = 1, gridExpand = TRUE)
addStyle(wb, "Table2_Descriptives", hs, rows = 5, cols = 1:ncol(desc_table_export), gridExpand = TRUE)

# Bold panel rows
panel_rows <- which(desc_table_export$Panel != "") + 5
if (length(panel_rows) > 0) {
  addStyle(wb, "Table2_Descriptives", panel_style, rows = panel_rows, cols = 1, gridExpand = TRUE)
}

setColWidths(wb, "Table2_Descriptives", cols = 1:ncol(desc_table_export), widths = "auto")
freezePane(wb, "Table2_Descriptives", firstActiveRow = 6)
saveWorkbook(wb, xlsx_file, overwrite = TRUE)

# ------------------------------------------------------------------------------
# 9) PRINT PREVIEW
# ------------------------------------------------------------------------------
cat("\nSaved descriptive statistics table to:\n")
cat("  CSV : ", csv_file, "\n", sep = "")
cat("  XLSX: ", xlsx_file, "\n", sep = "")

print(desc_table_export)

# Keep both in memory
assign("desc_table_core", desc_table_core, envir = .GlobalEnv)
assign("desc_table_core_export", desc_table_export, envir = .GlobalEnv)