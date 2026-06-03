# ===================== COMPLETE COMBINED DESCRIPTIVE STATISTICS PACK =====================
# PURPOSE:
#   Build the full descriptive-statistics package for the TSR mutual-fund analysis.
#
#   This script is a high-accuracy combined version of the descriptive scripts you
#   attached. It keeps the same analysis logic from the existing combined winsorized
#   script, but adds the two output groups that were missing from that combined file:
#
#     1) the monthly TSR adoption timing figure; and
#     2) the 24-month event-window coverage diagnostics, heatmap, and bar chart.
#
# WHAT THIS SCRIPT DOES:
#   - Starts from dt_24 already loaded in memory.
#   - Creates a local analysis copy only; it does NOT overwrite dt_24.
#   - Recreates the same helper variables used in the descriptive scripts.
#   - Computes Darendeli-style return-adjusted flow for descriptive reporting,
#     unless USE_DARENDELI_FLOW is set to FALSE.
#   - Winsorizes continuous descriptive variables at the 1st and 99th percentiles.
#   - Builds one complete Excel workbook with tables and figures.
#   - Also writes separate CSV/PNG files with clear, unique names for easy use in
#     the thesis appendix, notes to the professor, or LaTeX/Word tables.
#
# OUTPUT FOLDER:
#   The output folder stays the same as before:
#       file.path(PROJ_ROOT, "Descriptive Statistics")
#
# MAIN WORKBOOK OUTPUT:
#   descriptive_statistics_winsorized_complete_pack.xlsx
#
# WORKBOOK SHEETS:
#   01_Core_Descriptives
#   02_PrePost_Class
#   03_PrePost_N_Mean_Median
#   04_Class_Structure
#   05_TSR_Timing_Summary
#   06_TSR_Adoption_Monthly
#   07_TSR_Adoption_Figure
#   08_Pre_Retail_vs_Inst
#   09_Manager_Concentration
#   10_Top10_Managers
#   11_Quartile_Cutoffs
#   12_Fee_Quartile_Composition
#   13_Window_Coverage_Counts
#   14_Window_Heatmap_Table
#   15_Window_Heatmap_Figure
#   16_Window_Coverage_Bar
#
# ASSUMPTION:
#   The full pipeline has already run, so dt_24 exists in memory.
# =======================================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(fixest)
  library(openxlsx)
  library(ggplot2)
  library(scales)
})

# ------------------------------------------------------------------------------
# 0) USER OPTIONS
# ------------------------------------------------------------------------------
SAMPLE_OBJECT <- "dt_24"
OUT_WORKBOOK_NAME <- "descriptive_statistics_winsorized_complete_pack.xlsx"
USE_DARENDELI_FLOW <- TRUE
FLOW_VAR_NAME <- "flow_desc"
DIGITS <- 3

# Prefix used for all separate CSV/PNG outputs so the new files do not overwrite older runs.
OUTPUT_PREFIX <- "complete_winsorized"

# Within-FE SD: aligned with the main class-FE regressions
FE_ID_VAR   <- "crsp_fundno"
FE_TIME_VAR <- "ym"

# Alpha window used in the pre-period retail vs institutional comparison
ALPHA_WINDOW_COMPARE <- 12

# Winsorization rule used in the regression scripts
WINSOR_PROBS <- c(0.01, 0.99)

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

fmt_num <- function(x, digits = DIGITS) {
  ifelse(is.na(x), "", formatC(x, format = "f", digits = digits, big.mark = ","))
}

fmt_int <- function(x) {
  ifelse(is.na(x), "", formatC(as.integer(round(x)), format = "d", big.mark = ","))
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

safe_quantile <- function(x, p) {
  x <- suppressWarnings(as.numeric(x))
  x <- x[is.finite(x)]
  if (length(x) == 0) return(NA_real_)
  as.numeric(quantile(x, p, na.rm = TRUE, type = 7))
}

safe_ttest_p <- function(x, y) {
  x <- suppressWarnings(as.numeric(x))
  y <- suppressWarnings(as.numeric(y))
  x <- x[is.finite(x)]
  y <- y[is.finite(y)]
  if (length(x) < 2 || length(y) < 2) return(NA_real_)
  out <- tryCatch(t.test(x, y), error = function(e) NULL)
  if (is.null(out)) return(NA_real_)
  out$p.value
}

safe_ttest_stat <- function(x, y) {
  x <- suppressWarnings(as.numeric(x))
  y <- suppressWarnings(as.numeric(y))
  x <- x[is.finite(x)]
  y <- y[is.finite(y)]
  if (length(x) < 2 || length(y) < 2) return(NA_real_)
  out <- tryCatch(t.test(x, y), error = function(e) NULL)
  if (is.null(out)) return(NA_real_)
  unname(out$statistic)
}

p_to_stars <- function(p) {
  if (is.na(p)) return("")
  if (p < 0.001) return("***")
  if (p < 0.01)  return("**")
  if (p < 0.05)  return("*")
  if (p < 0.10)  return("+")
  ""
}

winsorize_vec <- function(x, probs = WINSOR_PROBS) {
  if (!is.numeric(x)) return(x)
  qs <- quantile(x, probs = probs, na.rm = TRUE, type = 7)
  if (!is.finite(qs[1]) || !is.finite(qs[2])) return(x)
  x[x < qs[1]] <- qs[1]
  x[x > qs[2]] <- qs[2]
  x
}

winsorize_dt <- function(DT, vars, probs = WINSOR_PROBS) {
  out <- copy(DT)
  vars <- intersect(vars, names(out))
  for (v in vars) {
    suppressWarnings(out[, (v) := as.numeric(get(v))])
    suppressWarnings(out[, (v) := winsorize_vec(get(v), probs = probs)])
  }
  out
}

make_quartile <- function(x) {
  x_num <- suppressWarnings(as.numeric(x))
  out <- rep(NA_integer_, length(x_num))
  ok <- is.finite(x_num)
  if (sum(ok) == 0) return(out)

  qs <- quantile(x_num[ok], probs = c(0, .25, .50, .75, 1), na.rm = TRUE, type = 7)
  brks <- unique(qs)
  if (length(brks) < 5) {
    r <- frank(x_num[ok], ties.method = "average", na.last = "keep")
    out[ok] <- pmin(4L, pmax(1L, ceiling(4 * r / max(r, na.rm = TRUE))))
    return(out)
  }
  out[ok] <- as.integer(cut(x_num[ok], breaks = qs, include.lowest = TRUE, labels = FALSE))
  out
}

write_table_sheet <- function(wb, sheet_name, title_text, dt_to_write, start_row = 4) {
  addWorksheet(wb, sheet_name)
  writeData(wb, sheet_name, title_text, startRow = 1, startCol = 1)
  writeData(wb, sheet_name, dt_to_write, startRow = start_row, startCol = 1, colNames = TRUE)

  title_style <- createStyle(textDecoration = "bold", fontSize = 12, wrapText = TRUE)
  head_style  <- createStyle(textDecoration = "bold", halign = "center", valign = "center")
  panel_style <- createStyle(textDecoration = "bold")

  addStyle(wb, sheet_name, title_style, rows = 1, cols = 1, gridExpand = TRUE)
  addStyle(wb, sheet_name, head_style, rows = start_row, cols = 1:ncol(dt_to_write), gridExpand = TRUE)

  if ("Panel" %in% names(dt_to_write)) {
    panel_rows <- which(dt_to_write$Panel != "") + start_row
    if (length(panel_rows) > 0) {
      addStyle(wb, sheet_name, panel_style, rows = panel_rows, cols = 1, gridExpand = TRUE)
    }
  }

  setColWidths(wb, sheet_name, cols = 1:ncol(dt_to_write), widths = "auto")
  freezePane(wb, sheet_name, firstActiveRow = start_row + 1)
}

# This small helper writes a figure into its own Excel sheet. Keeping figures in
# separate sheets makes it easier to inspect and export them later.
write_image_sheet <- function(wb, sheet_name, title_text, image_path,
                              width = 9, height = 5) {
  addWorksheet(wb, sheet_name)
  writeData(wb, sheet_name, title_text, startRow = 1, startCol = 1)

  title_style <- createStyle(textDecoration = "bold", fontSize = 12, wrapText = TRUE)
  addStyle(wb, sheet_name, title_style, rows = 1, cols = 1, gridExpand = TRUE)
  setColWidths(wb, sheet_name, cols = 1, widths = 80)

  if (!is.na(image_path) && file.exists(image_path)) {
    insertImage(wb, sheet_name, image_path,
                startRow = 4, startCol = 1,
                width = width, height = height, units = "in")
  } else {
    writeData(wb, sheet_name,
              "Figure was not created. Check whether the source data were available.",
              startRow = 4, startCol = 1)
  }
}

# Standalone CSV exports are useful because Word/Excel/LaTeX workflows often need
# individual table files rather than one large workbook.
save_table_csv <- function(dt_to_save, filename) {
  out_file <- file.path(OUT_DIR, filename)
  fwrite(dt_to_save, out_file)
  out_file
}

# ------------------------------------------------------------------------------
# 3) BUILD THE COMMON ANALYSIS COPY USED BY ALL TABLES
# ------------------------------------------------------------------------------
# Month fixed effect
if (!("ym" %in% names(DT))) {
  DT[, ym := format(caldt, "%Y-%m")]
}

# Clean Y/N style indicators
if (!("retail_ind" %in% names(DT)) && "retail_fund" %in% names(DT)) {
  DT[, retail_ind := yn_to_num(retail_fund)]
}
if (!("inst_ind" %in% names(DT)) && "inst_fund" %in% names(DT)) {
  DT[, inst_ind := yn_to_num(inst_fund)]
}

# Clean class groups used in the original scripts
if (!all(c("retail_fund", "inst_fund") %in% names(DT))) {
  stop("retail_fund and/or inst_fund not found in sample.")
}
DT[, retail_num := yn_to_num(retail_fund)]
DT[, inst_num   := yn_to_num(inst_fund)]
DT[, class_group := fifelse(retail_num == 1 & (is.na(inst_num) | inst_num != 1), "Retail",
                            fifelse(inst_num == 1 & (is.na(retail_num) | retail_num != 1), "Institutional",
                                    "Other"))]

# TSR indicators and pre/post period
if (!("tsr_filingdate_first_mend" %in% names(DT))) {
  if ("tsr_filingdate_first" %in% names(DT)) {
    DT[, tsr_filingdate_first := as.Date(tsr_filingdate_first)]
    DT[, tsr_filingdate_first_mend := ceiling_date(tsr_filingdate_first, "month") - days(1)]
  } else {
    stop("Neither tsr_filingdate_first_mend nor tsr_filingdate_first exists in sample.")
  }
}

if ("tsr_filingdate_first" %in% names(DT)) {
  DT[, tsr_filingdate_first := as.Date(tsr_filingdate_first)]
  DT[, tsr_filing_observed := as.numeric(!is.na(tsr_filingdate_first))]
} else {
  DT[, tsr_filingdate_first := as.Date(NA)]
  DT[, tsr_filing_observed := NA_real_]
}

if ("tsr_reportdate_first" %in% names(DT)) {
  DT[, tsr_reportdate_first := as.Date(tsr_reportdate_first)]
  DT[, tsr_report_observed := as.numeric(!is.na(tsr_reportdate_first))]
} else {
  DT[, tsr_reportdate_first := as.Date(NA)]
  DT[, tsr_report_observed := NA_real_]
}

DT[, post_tsr := as.numeric(!is.na(tsr_filingdate_first_mend) & caldt >= tsr_filingdate_first_mend)]
DT[, period := fifelse(post_tsr == 1, "Post", "Pre")]
DT[, post_fund := post_tsr]

# Darendeli-style flow for descriptives, same logic used in the original descriptive scripts
if (isTRUE(USE_DARENDELI_FLOW)) {
  req_flow <- c("crsp_fundno", "mtna", "mret")
  miss_flow <- setdiff(req_flow, names(DT))
  if (length(miss_flow) > 0) {
    stop("Cannot build Darendeli flow. Missing columns: ", paste(miss_flow, collapse = ", "))
  }

  DT[, mtna := as.numeric(mtna)]
  DT[, mret := as.numeric(mret)]
  setorder(DT, crsp_fundno, caldt)
  DT[, mtna_l1_desc := shift(mtna, 1L), by = crsp_fundno]

  if (is.finite(mean(abs(DT$mret), na.rm = TRUE)) && mean(abs(DT$mret), na.rm = TRUE) > 1) {
    DT[, mret := mret / 100]
  }

  DT[, (FLOW_VAR_NAME) := (mtna - mtna_l1_desc * (1 + mret)) / (mtna_l1_desc * (1 + mret))]
  DT[is.na(mtna_l1_desc) | mtna_l1_desc <= 0 | is.na(mret) | (1 + mret) <= 0, (FLOW_VAR_NAME) := NA_real_]
} else {
  if (!("flow" %in% names(DT))) stop("flow not found and USE_DARENDELI_FLOW = FALSE.")
  DT[, (FLOW_VAR_NAME) := as.numeric(flow)]
}

# Ensure expected descriptive variables are numeric when present
numeric_candidates <- c(
  FLOW_VAR_NAME,
  "alpha_capm_12m", "alpha_capm_24m", "alpha_capm_36m",
  "alpha_carhart_12m", "alpha_carhart_24m", "alpha_carhart_36m",
  "fund_ret_12m", "fund_ret_60m",
  "age_years_l1", "log_tna_l1", "log_familytna_l1",
  "turn_ratio_l1", "mgmt_fee_l1", "exp_ratio_l1",
  "mtna", "mret"
)
for (v in intersect(numeric_candidates, names(DT))) {
  suppressWarnings(DT[, (v) := as.numeric(get(v))])
}

# This is the central change: all descriptive-statistics tables below use DT_WIN.
# Identifiers, dates, period flags, and 0/1 indicators are not winsorized.
winsor_vars <- intersect(c(
  FLOW_VAR_NAME,
  "alpha_capm_12m", "alpha_capm_24m", "alpha_capm_36m",
  "alpha_carhart_12m", "alpha_carhart_24m", "alpha_carhart_36m",
  "fund_ret_12m", "fund_ret_60m",
  "age_years_l1", "log_tna_l1", "log_familytna_l1",
  "turn_ratio_l1", "mgmt_fee_l1", "exp_ratio_l1"
), names(DT))

DT_WIN <- winsorize_dt(DT, winsor_vars, probs = WINSOR_PROBS)
assign("DT_desc_winsorized", DT_WIN, envir = .GlobalEnv)

# Use the winsorized copy for all downstream descriptive tables.
DT <- DT_WIN

# ------------------------------------------------------------------------------
# 4) TABLE 01: CORE DESCRIPTIVE STATISTICS
# ------------------------------------------------------------------------------
var_map_core <- data.table(
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
var_map_core <- var_map_core[var %in% names(DT)]
if (nrow(var_map_core) == 0) stop("No requested variables found in sample for core descriptives.")

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

summarize_core_one <- function(DT, var, label, scale = 1, within_fe = TRUE) {
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
  lapply(seq_len(nrow(var_map_core)), function(i) {
    out <- summarize_core_one(
      DT = DT,
      var = var_map_core$var[i],
      label = var_map_core$label[i],
      scale = var_map_core$scale[i],
      within_fe = var_map_core$within_fe[i]
    )
    out[, Panel := var_map_core$panel[i]]
    out
  }),
  fill = TRUE
)
setcolorder(desc_table_core,
            c("Panel", "Variable", "N", "Mean", "SD", "P25", "Median", "P75", "Within-FE SD"))

desc_table_core_export <- copy(desc_table_core)
desc_table_core_export[, N := fmt_int(N)]
for (cc in c("Mean", "SD", "P25", "Median", "P75", "Within-FE SD")) {
  desc_table_core_export[, (cc) := fmt_num(get(cc), digits = DIGITS)]
}
for (pp in unique(desc_table_core_export$Panel)) {
  idx <- which(desc_table_core_export$Panel == pp)
  if (length(idx) > 1) desc_table_core_export$Panel[idx[-1]] <- ""
}

# ------------------------------------------------------------------------------
# 5) TABLE 02: PRE/POST BY CLASS GROUP, WITH DIFFERENCES AND STARS
# ------------------------------------------------------------------------------
var_specs_prepost_class <- data.table(
  var = c(
    FLOW_VAR_NAME,
    "alpha_capm_12m", "alpha_capm_24m", "alpha_capm_36m",
    "alpha_carhart_12m", "alpha_carhart_24m", "alpha_carhart_36m",
    "age_years_l1",
    "log_tna_l1",
    "log_familytna_l1",
    "turn_ratio_l1",
    "mgmt_fee_l1",
    "exp_ratio_l1"
  ),
  label = c(
    "Flow",
    "CAPM alpha (12 months)",
    "CAPM alpha (24 months)",
    "CAPM alpha (36 months)",
    "Carhart alpha (12 months)",
    "Carhart alpha (24 months)",
    "Carhart alpha (36 months)",
    "Age (t-1)",
    "Log(TNA) (t-1)",
    "Log(FamilyTNA) (t-1)",
    "Turnover ratio (t-1)",
    "Management fee (t-1)",
    "Expense ratio (t-1)"
  ),
  scale = c(
    100, 100, 100, 100, 100, 100, 100,
    1, 1, 1,
    100, 100, 100
  )
)
var_specs_prepost_class <- var_specs_prepost_class[var %in% names(DT)]
if (nrow(var_specs_prepost_class) == 0) stop("None of the requested variables are present for pre/post class table.")

panel_A_raw <- rbindlist(list(
  data.table(
    Panel = "Panel A. Sample composition",
    Variable = "Observations",
    Overall_Pre = nrow(DT[period == "Pre"]),
    Overall_Post = nrow(DT[period == "Post"]),
    Overall_Diff = nrow(DT[period == "Post"]) - nrow(DT[period == "Pre"]),
    Retail_Pre = nrow(DT[class_group == "Retail" & period == "Pre"]),
    Retail_Post = nrow(DT[class_group == "Retail" & period == "Post"]),
    Retail_Diff = nrow(DT[class_group == "Retail" & period == "Post"]) - nrow(DT[class_group == "Retail" & period == "Pre"]),
    Inst_Pre = nrow(DT[class_group == "Institutional" & period == "Pre"]),
    Inst_Post = nrow(DT[class_group == "Institutional" & period == "Post"]),
    Inst_Diff = nrow(DT[class_group == "Institutional" & period == "Post"]) - nrow(DT[class_group == "Institutional" & period == "Pre"])
  ),
  data.table(
    Panel = "",
    Variable = "Share classes",
    Overall_Pre = uniqueN(DT[period == "Pre", crsp_fundno]),
    Overall_Post = uniqueN(DT[period == "Post", crsp_fundno]),
    Overall_Diff = uniqueN(DT[period == "Post", crsp_fundno]) - uniqueN(DT[period == "Pre", crsp_fundno]),
    Retail_Pre = uniqueN(DT[class_group == "Retail" & period == "Pre", crsp_fundno]),
    Retail_Post = uniqueN(DT[class_group == "Retail" & period == "Post", crsp_fundno]),
    Retail_Diff = uniqueN(DT[class_group == "Retail" & period == "Post", crsp_fundno]) - uniqueN(DT[class_group == "Retail" & period == "Pre", crsp_fundno]),
    Inst_Pre = uniqueN(DT[class_group == "Institutional" & period == "Pre", crsp_fundno]),
    Inst_Post = uniqueN(DT[class_group == "Institutional" & period == "Post", crsp_fundno]),
    Inst_Diff = uniqueN(DT[class_group == "Institutional" & period == "Post", crsp_fundno]) - uniqueN(DT[class_group == "Institutional" & period == "Pre", crsp_fundno])
  ),
  data.table(
    Panel = "",
    Variable = "Portfolios",
    Overall_Pre = uniqueN(DT[period == "Pre", crsp_portno]),
    Overall_Post = uniqueN(DT[period == "Post", crsp_portno]),
    Overall_Diff = uniqueN(DT[period == "Post", crsp_portno]) - uniqueN(DT[period == "Pre", crsp_portno]),
    Retail_Pre = uniqueN(DT[class_group == "Retail" & period == "Pre", crsp_portno]),
    Retail_Post = uniqueN(DT[class_group == "Retail" & period == "Post", crsp_portno]),
    Retail_Diff = uniqueN(DT[class_group == "Retail" & period == "Post", crsp_portno]) - uniqueN(DT[class_group == "Retail" & period == "Pre", crsp_portno]),
    Inst_Pre = uniqueN(DT[class_group == "Institutional" & period == "Pre", crsp_portno]),
    Inst_Post = uniqueN(DT[class_group == "Institutional" & period == "Post", crsp_portno]),
    Inst_Diff = uniqueN(DT[class_group == "Institutional" & period == "Post", crsp_portno]) - uniqueN(DT[class_group == "Institutional" & period == "Pre", crsp_portno])
  )
), fill = TRUE)

build_mean_row <- function(D, v, lab, scl = 1, panel_name = "") {
  x_all_pre  <- D[period == "Pre", get(v)]
  x_all_post <- D[period == "Post", get(v)]

  x_ret_pre  <- D[class_group == "Retail" & period == "Pre", get(v)]
  x_ret_post <- D[class_group == "Retail" & period == "Post", get(v)]

  x_ins_pre  <- D[class_group == "Institutional" & period == "Pre", get(v)]
  x_ins_post <- D[class_group == "Institutional" & period == "Post", get(v)]

  all_p <- safe_ttest_p(x_all_pre, x_all_post)
  ret_p <- safe_ttest_p(x_ret_pre, x_ret_post)
  ins_p <- safe_ttest_p(x_ins_pre, x_ins_post)

  data.table(
    Panel = panel_name,
    Variable = lab,
    Overall_Pre = safe_mean(x_all_pre) * scl,
    Overall_Post = safe_mean(x_all_post) * scl,
    Overall_Diff = safe_mean(x_all_post) * scl - safe_mean(x_all_pre) * scl,
    Overall_Stars = p_to_stars(all_p),
    Retail_Pre = safe_mean(x_ret_pre) * scl,
    Retail_Post = safe_mean(x_ret_post) * scl,
    Retail_Diff = safe_mean(x_ret_post) * scl - safe_mean(x_ret_pre) * scl,
    Retail_Stars = p_to_stars(ret_p),
    Inst_Pre = safe_mean(x_ins_pre) * scl,
    Inst_Post = safe_mean(x_ins_post) * scl,
    Inst_Diff = safe_mean(x_ins_post) * scl - safe_mean(x_ins_pre) * scl,
    Inst_Stars = p_to_stars(ins_p)
  )
}

panel_B_raw <- rbindlist(
  lapply(seq_len(nrow(var_specs_prepost_class)), function(i) {
    build_mean_row(
      D = DT,
      v = var_specs_prepost_class$var[i],
      lab = var_specs_prepost_class$label[i],
      scl = var_specs_prepost_class$scale[i],
      panel_name = if (i == 1) "Panel B. Mean characteristics by period and class" else ""
    )
  }),
  fill = TRUE
)

panel_A_export <- copy(panel_A_raw)
for (cc in c("Overall_Pre", "Overall_Post", "Overall_Diff",
             "Retail_Pre", "Retail_Post", "Retail_Diff",
             "Inst_Pre", "Inst_Post", "Inst_Diff")) {
  panel_A_export[, (cc) := fmt_int(get(cc))]
}

panel_B_export <- copy(panel_B_raw)
for (cc in c("Overall_Pre", "Overall_Post",
             "Retail_Pre", "Retail_Post",
             "Inst_Pre", "Inst_Post")) {
  panel_B_export[, (cc) := fmt_num(get(cc), digits = DIGITS)]
}
panel_B_export[, Overall_Diff := paste0(fmt_num(Overall_Diff, digits = DIGITS), Overall_Stars)]
panel_B_export[, Retail_Diff := paste0(fmt_num(Retail_Diff, digits = DIGITS), Retail_Stars)]
panel_B_export[, Inst_Diff := paste0(fmt_num(Inst_Diff, digits = DIGITS), Inst_Stars)]
panel_B_export[, c("Overall_Stars", "Retail_Stars", "Inst_Stars") := NULL]

one_informative_descriptive_table_export <- rbindlist(list(panel_A_export, panel_B_export), fill = TRUE)
one_informative_descriptive_table_raw <- rbindlist(list(panel_A_raw, panel_B_raw), fill = TRUE)
keep_cols_prepost_class <- c("Panel", "Variable",
                             "Overall_Pre", "Overall_Post", "Overall_Diff",
                             "Retail_Pre", "Retail_Post", "Retail_Diff",
                             "Inst_Pre", "Inst_Post", "Inst_Diff")
one_informative_descriptive_table_export <- one_informative_descriptive_table_export[, ..keep_cols_prepost_class]

# ------------------------------------------------------------------------------
# 6) TABLE 03: PRE/POST N, MEAN, MEDIAN WITH DIFFERENCES
# ------------------------------------------------------------------------------
var_map_prepost_n <- data.table(
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
var_map_prepost_n <- var_map_prepost_n[var %in% names(DT)]
if (nrow(var_map_prepost_n) == 0) stop("No requested variables found in the sample for pre/post N/mean/median table.")

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
  lapply(seq_len(nrow(var_map_prepost_n)), function(i) {
    summarize_pre_post_one(
      DT = DT,
      var = var_map_prepost_n$var[i],
      label = var_map_prepost_n$label[i],
      panel = var_map_prepost_n$panel[i],
      scale = var_map_prepost_n$scale[i]
    )
  }),
  fill = TRUE
)

desc_table_pre_post_with_diffs_export <- copy(desc_table_pre_post_with_diffs_raw)
for (pp in unique(desc_table_pre_post_with_diffs_export$Panel)) {
  idx <- which(desc_table_pre_post_with_diffs_export$Panel == pp)
  if (length(idx) > 1) desc_table_pre_post_with_diffs_export$Panel[idx[-1]] <- ""
}
for (cc in c("N_Pre", "N_Post", "N_Diff")) {
  desc_table_pre_post_with_diffs_export[, (cc) := fmt_int(get(cc))]
}
for (cc in c("Mean_Pre", "Mean_Post", "Mean_Diff",
             "Median_Pre", "Median_Post", "Median_Diff")) {
  desc_table_pre_post_with_diffs_export[, (cc) := fmt_num(get(cc), digits = DIGITS)]
}

# ------------------------------------------------------------------------------
# 7) TABLES 04-11: REST OF DESCRIPTIVE PACK
# ------------------------------------------------------------------------------
# Portfolio-level unique timing file from the winsorized sample copy.
# Winsorization does not alter these identifiers/dates; this keeps all tables built from one copy.
port_dates_cols <- intersect(
  c("crsp_portno", "tsr_filingdate_first", "tsr_reportdate_first", "tsr_filingdate_first_mend", "pre_months", "post_months"),
  names(DT)
)
port_dates <- unique(DT[, ..port_dates_cols])
setorder(port_dates, crsp_portno)

# Latest class snapshot for cross-sectional structure tables
class_snapshot <- copy(DT)
setorder(class_snapshot, crsp_fundno, -caldt)
class_snapshot <- class_snapshot[!is.na(crsp_fundno), .SD[1], by = crsp_fundno]

if (!("mgmt_cd" %in% names(class_snapshot))) {
  stop("mgmt_cd not found in sample.")
}
if (!("mgmt_name" %in% names(class_snapshot))) {
  class_snapshot[, mgmt_name := NA_character_]
  DT[, mgmt_name := NA_character_]
}

# TABLE 04: CLASS STRUCTURE
n_share_classes <- uniqueN(class_snapshot$crsp_fundno)
n_portfolios <- uniqueN(class_snapshot$crsp_portno)
n_retail_classes <- uniqueN(class_snapshot[class_group == "Retail", crsp_fundno])
n_institutional_classes <- uniqueN(class_snapshot[class_group == "Institutional", crsp_fundno])

portfolio_class_counts <- class_snapshot[
  , .(
    n_share_classes = uniqueN(crsp_fundno),
    n_retail_classes = uniqueN(crsp_fundno[class_group == "Retail"]),
    n_institutional_classes = uniqueN(crsp_fundno[class_group == "Institutional"])
  ),
  by = crsp_portno
]

n_mixed_portfolios <- portfolio_class_counts[n_retail_classes > 0 & n_institutional_classes > 0, .N]

class_structure_table <- data.table(
  Metric = c(
    "Unique share classes",
    "Unique portfolios",
    "Retail share classes (clean)",
    "Institutional share classes (clean)",
    "Portfolios with both retail and institutional classes",
    "Average share classes per portfolio",
    "Average retail classes per portfolio",
    "Average institutional classes per portfolio"
  ),
  Value = c(
    n_share_classes,
    n_portfolios,
    n_retail_classes,
    n_institutional_classes,
    n_mixed_portfolios,
    mean(portfolio_class_counts$n_share_classes, na.rm = TRUE),
    mean(portfolio_class_counts$n_retail_classes, na.rm = TRUE),
    mean(portfolio_class_counts$n_institutional_classes, na.rm = TRUE)
  )
)
class_structure_table[, Value := ifelse(grepl("Average", Metric), fmt_num(Value), fmt_int(Value))]

# TABLE 05: TSR TIMING SUMMARY
port_dates[, lag_days := as.integer(as.Date(tsr_filingdate_first) - as.Date(tsr_reportdate_first))]

tsr_timing_summary <- data.table(
  Metric = c(
    "Portfolios with first TSR filing date",
    "Portfolios with first TSR report date",
    "Mean lag (filing date - report date, days)",
    "Median lag (days)",
    "Minimum lag (days)",
    "25th percentile lag (days)",
    "75th percentile lag (days)",
    "Maximum lag (days)"
  ),
  Value = c(
    uniqueN(port_dates[!is.na(tsr_filingdate_first), crsp_portno]),
    uniqueN(port_dates[!is.na(tsr_reportdate_first), crsp_portno]),
    safe_mean(port_dates$lag_days),
    safe_median(port_dates$lag_days),
    safe_quantile(port_dates$lag_days, 0),
    safe_quantile(port_dates$lag_days, 0.25),
    safe_quantile(port_dates$lag_days, 0.75),
    safe_quantile(port_dates$lag_days, 1)
  )
)
tsr_timing_summary[, Value := ifelse(grepl("Portfolios", Metric), fmt_int(Value), fmt_num(Value))]

# TABLE 06: MONTHLY TSR ADOPTION TABLE
port_dates[, filing_monthend := as.Date(ceiling_date(as.Date(tsr_filingdate_first), "month") - days(1))]
port_dates[, report_monthend := as.Date(ceiling_date(as.Date(tsr_reportdate_first), "month") - days(1))]

filing_monthly <- port_dates[!is.na(filing_monthend), .(First_TSR_Filings = .N), by = filing_monthend]
report_monthly <- port_dates[!is.na(report_monthend), .(First_TSR_Reports = .N), by = report_monthend]

setnames(filing_monthly, "filing_monthend", "Month")
setnames(report_monthly, "report_monthend", "Month")

tsr_adoption_monthly <- merge(filing_monthly, report_monthly, by = "Month", all = TRUE)
setorder(tsr_adoption_monthly, Month)
for (cc in c("First_TSR_Filings", "First_TSR_Reports")) {
  tsr_adoption_monthly[is.na(get(cc)), (cc) := 0L]
}
tsr_adoption_monthly[, Month := as.character(Month)]

# TABLE 07: PRE-PERIOD RETAIL VS INSTITUTIONAL COMPARISON
capm_var <- paste0("alpha_capm_", ALPHA_WINDOW_COMPARE, "m")
carhart_var <- paste0("alpha_carhart_", ALPHA_WINDOW_COMPARE, "m")

compare_map <- data.table(
  var = c(FLOW_VAR_NAME, capm_var, carhart_var,
          "age_years_l1", "log_tna_l1", "log_familytna_l1",
          "turn_ratio_l1", "mgmt_fee_l1", "exp_ratio_l1"),
  label = c(
    "Flow",
    paste0("CAPM alpha (", ALPHA_WINDOW_COMPARE, " months)"),
    paste0("Carhart alpha (", ALPHA_WINDOW_COMPARE, " months)"),
    "Age (t-1)",
    "Log(TNA) (t-1)",
    "Log(FamilyTNA) (t-1)",
    "Turnover ratio (t-1)",
    "Management fee (t-1)",
    "Expense ratio (t-1)"
  ),
  scale = c(100, 100, 100, 1, 1, 1, 100, 100, 100)
)
compare_map <- compare_map[var %in% names(DT)]

pre_compare_dt <- DT[
  class_group %in% c("Retail", "Institutional") &
    !is.na(tsr_filingdate_first_mend) &
    caldt < tsr_filingdate_first_mend
]

pre_retail_vs_inst <- rbindlist(
  lapply(seq_len(nrow(compare_map)), function(i) {
    v <- compare_map$var[i]
    lab <- compare_map$label[i]
    scl <- compare_map$scale[i]

    xr <- suppressWarnings(as.numeric(pre_compare_dt[class_group == "Retail", get(v)]))
    xi <- suppressWarnings(as.numeric(pre_compare_dt[class_group == "Institutional", get(v)]))

    data.table(
      Variable = lab,
      N_Retail = sum(!is.na(xr)),
      Retail_Mean = safe_mean(xr) * scl,
      N_Institutional = sum(!is.na(xi)),
      Institutional_Mean = safe_mean(xi) * scl,
      Difference_Retail_minus_Institutional = (safe_mean(xr) - safe_mean(xi)) * scl,
      `t-stat` = safe_ttest_stat(xr, xi),
      `p-value` = safe_ttest_p(xr, xi)
    )
  }),
  fill = TRUE
)
for (cc in c("N_Retail", "N_Institutional")) pre_retail_vs_inst[, (cc) := fmt_int(get(cc))]
for (cc in c("Retail_Mean", "Institutional_Mean", "Difference_Retail_minus_Institutional", "t-stat", "p-value")) {
  pre_retail_vs_inst[, (cc) := fmt_num(get(cc))]
}

# TABLE 08: MANAGEMENT-COMPANY CONCENTRATION SUMMARY
manager_snapshot <- class_snapshot[!is.na(mgmt_cd)]

classes_per_manager <- manager_snapshot[, .(n_share_classes = uniqueN(crsp_fundno)), by = .(mgmt_cd, mgmt_name)]
obs_per_manager <- DT[!is.na(mgmt_cd), .(n_obs = .N), by = .(mgmt_cd, mgmt_name)]
tna_per_manager <- manager_snapshot[!is.na(mtna), .(snapshot_tna = sum(as.numeric(mtna), na.rm = TRUE)), by = .(mgmt_cd, mgmt_name)]

manager_conc <- Reduce(function(x, y) merge(x, y, by = c("mgmt_cd", "mgmt_name"), all = TRUE),
                       list(classes_per_manager, obs_per_manager, tna_per_manager))
for (cc in c("n_share_classes", "n_obs", "snapshot_tna")) manager_conc[is.na(get(cc)), (cc) := 0]

setorder(manager_conc, -n_obs, -snapshot_tna)

top10_obs_share <- 100 * manager_conc[1:min(10, .N), sum(n_obs)] / manager_conc[, sum(n_obs)]
top10_tna_share <- 100 * manager_conc[1:min(10, .N), sum(snapshot_tna)] / manager_conc[, sum(snapshot_tna)]

manager_concentration_summary <- data.table(
  Metric = c(
    "Management companies",
    "Mean share classes per management company",
    "Median share classes per management company",
    "Top 10 management companies' share of observations (%)",
    "Top 10 management companies' share of snapshot TNA (%)"
  ),
  Value = c(
    uniqueN(manager_conc$mgmt_cd),
    mean(manager_conc$n_share_classes, na.rm = TRUE),
    median(manager_conc$n_share_classes, na.rm = TRUE),
    top10_obs_share,
    top10_tna_share
  )
)
manager_concentration_summary[, Value := ifelse(Metric == "Management companies", fmt_int(Value), fmt_num(Value))]

# TABLE 09: TOP 10 MANAGEMENT COMPANIES
manager_conc[, obs_share_pct := 100 * n_obs / sum(n_obs)]
manager_conc[, tna_share_pct := 100 * snapshot_tna / sum(snapshot_tna)]

top10_managers <- copy(manager_conc[1:min(10, .N),
                                    .(mgmt_cd, mgmt_name, n_share_classes, n_obs, obs_share_pct, snapshot_tna, tna_share_pct)])
top10_managers[, n_share_classes := fmt_int(n_share_classes)]
top10_managers[, n_obs := fmt_int(n_obs)]
top10_managers[, obs_share_pct := fmt_num(obs_share_pct)]
top10_managers[, snapshot_tna := fmt_num(snapshot_tna)]
top10_managers[, tna_share_pct := fmt_num(tna_share_pct)]

# TABLE 10: QUARTILE CUTOFFS FOR FEES / SIZE / TURNOVER
# Uses the latest class-level snapshot, after winsorizing the continuous variables above.
dist_snapshot <- copy(class_snapshot)

quartile_cutoffs <- rbindlist(list(
  data.table(
    Variable = "Expense ratio (t-1)",
    Min = safe_quantile(dist_snapshot$exp_ratio_l1, 0) * 100,
    P25 = safe_quantile(dist_snapshot$exp_ratio_l1, 0.25) * 100,
    Median = safe_quantile(dist_snapshot$exp_ratio_l1, 0.50) * 100,
    P75 = safe_quantile(dist_snapshot$exp_ratio_l1, 0.75) * 100,
    Max = safe_quantile(dist_snapshot$exp_ratio_l1, 1) * 100
  ),
  data.table(
    Variable = "Log(TNA) (t-1)",
    Min = safe_quantile(dist_snapshot$log_tna_l1, 0),
    P25 = safe_quantile(dist_snapshot$log_tna_l1, 0.25),
    Median = safe_quantile(dist_snapshot$log_tna_l1, 0.50),
    P75 = safe_quantile(dist_snapshot$log_tna_l1, 0.75),
    Max = safe_quantile(dist_snapshot$log_tna_l1, 1)
  ),
  data.table(
    Variable = "Turnover ratio (t-1)",
    Min = safe_quantile(dist_snapshot$turn_ratio_l1, 0) * 100,
    P25 = safe_quantile(dist_snapshot$turn_ratio_l1, 0.25) * 100,
    Median = safe_quantile(dist_snapshot$turn_ratio_l1, 0.50) * 100,
    P75 = safe_quantile(dist_snapshot$turn_ratio_l1, 0.75) * 100,
    Max = safe_quantile(dist_snapshot$turn_ratio_l1, 1) * 100
  )
), fill = TRUE)
for (cc in c("Min", "P25", "Median", "P75", "Max")) quartile_cutoffs[, (cc) := fmt_num(get(cc))]

# TABLE 11: FEE-QUARTILE COMPOSITION TABLE
fee_comp <- copy(class_snapshot)
fee_comp <- fee_comp[!is.na(exp_ratio_l1)]
fee_comp[, fee_quartile := make_quartile(exp_ratio_l1)]

fee_quartile_composition <- fee_comp[
  !is.na(fee_quartile),
  .(
    N_classes = uniqueN(crsp_fundno),
    Mean_expense_ratio = safe_mean(exp_ratio_l1) * 100,
    Mean_logTNA = safe_mean(log_tna_l1),
    Median_logTNA = safe_median(log_tna_l1),
    Mean_turnover = safe_mean(turn_ratio_l1) * 100,
    Median_turnover = safe_median(turn_ratio_l1) * 100,
    Pct_retail = 100 * mean(class_group == "Retail", na.rm = TRUE),
    Pct_institutional = 100 * mean(class_group == "Institutional", na.rm = TRUE)
  ),
  by = fee_quartile
][order(fee_quartile)]

fee_quartile_composition[, fee_quartile := paste0("Q", fee_quartile)]
setnames(fee_quartile_composition, "fee_quartile", "Expense ratio quartile")
fee_quartile_composition[, N_classes := fmt_int(N_classes)]
for (cc in setdiff(names(fee_quartile_composition), c("Expense ratio quartile", "N_classes"))) {
  fee_quartile_composition[, (cc) := fmt_num(get(cc))]
}

# ------------------------------------------------------------------------------
# 8) FIGURES AND WINDOW-COVERAGE DIAGNOSTICS
# ------------------------------------------------------------------------------
# The original combined file already kept the monthly TSR adoption table, but it
# did not recreate the visual timing figure. This figure compares the first TSR
# filing month and the first TSR report month at the portfolio level.
tsr_figure_path <- file.path(
  OUT_DIR,
  paste0("figure_07_tsr_adoption_timing_", OUTPUT_PREFIX, ".png")
)

fig_dt <- copy(tsr_adoption_monthly)
fig_dt[, Month := as.Date(Month)]

fig_long <- rbindlist(list(
  fig_dt[, .(Month, Count = First_TSR_Filings, Series = "First TSR filings")],
  fig_dt[, .(Month, Count = First_TSR_Reports, Series = "First TSR reports")]
), fill = TRUE)

p_tsr_adoption <- ggplot(fig_long, aes(x = Month, y = Count, color = Series)) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 1.7) +
  labs(
    title = "Monthly first TSR filings and reports",
    x = NULL,
    y = "Number of portfolios",
    color = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "top"
  )

ggsave(filename = tsr_figure_path, plot = p_tsr_adoption, width = 9, height = 5, dpi = 300)

# The event-window diagnostics come from the separate 24-window heatmap script.
# They are class-level diagnostics: one row per crsp_fundno. The purpose is to
# show whether each share class has the full 24 months before and 24 months after
# the portfolio-specific TSR filing month-end.
window_dt <- copy(DT)
window_dt[, caldt := as.Date(caldt)]
window_dt[, tsr_filingdate_first_mend := as.Date(tsr_filingdate_first_mend)]
window_dt <- window_dt[
  !is.na(crsp_fundno) &
    !is.na(caldt) &
    !is.na(tsr_filingdate_first_mend)
]

class_window_summary <- window_dt[, .(
  first_month = min(caldt, na.rm = TRUE),
  last_month  = max(caldt, na.rm = TRUE),
  n_rows      = .N,
  n_months    = uniqueN(caldt),
  pre_months  = uniqueN(caldt[caldt <  tsr_filingdate_first_mend]),
  post_months = uniqueN(caldt[caldt >= tsr_filingdate_first_mend]),
  crsp_portno = if (uniqueN(crsp_portno, na.rm = TRUE) == 1) {
    unique(na.omit(crsp_portno))[1]
  } else {
    NA_real_
  }
), by = crsp_fundno]

# dt_24 should already be no wider than 24 months on either side. The cap below is
# kept as a safety check so the heatmap always has a clean 0-to-24 interpretation.
class_window_summary[, pre_months  := pmin(pre_months, 24L)]
class_window_summary[, post_months := pmin(post_months, 24L)]

class_window_summary[, coverage_group := fifelse(
  pre_months == 24 & post_months == 24, "Full 24 pre + 24 post",
  fifelse(
    pre_months == 24 & post_months < 24, "Full pre, incomplete post",
    fifelse(
      pre_months < 24 & post_months == 24, "Incomplete pre, full post",
      "Incomplete pre and post"
    )
  )
)]

class_window_summary[, coverage_group := factor(
  coverage_group,
  levels = c(
    "Full 24 pre + 24 post",
    "Full pre, incomplete post",
    "Incomplete pre, full post",
    "Incomplete pre and post"
  )
)]

coverage_counts_table <- class_window_summary[, .(
  n_classes = .N,
  pct_classes = 100 * .N / nrow(class_window_summary)
), by = coverage_group][order(coverage_group)]

coverage_counts_table_export <- copy(coverage_counts_table)
coverage_counts_table_export[, coverage_group := as.character(coverage_group)]
coverage_counts_table_export[, n_classes := fmt_int(n_classes)]
coverage_counts_table_export[, pct_classes := fmt_num(pct_classes)]

heat_dt <- class_window_summary[, .N, by = .(pre_months, post_months)]
full_grid <- CJ(pre_months = 0:24, post_months = 0:24)
heat_dt <- heat_dt[full_grid, on = .(pre_months, post_months)]
heat_dt[is.na(N), N := 0L]

pre_post_table <- dcast(
  heat_dt,
  post_months ~ pre_months,
  value.var = "N",
  fill = 0
)

class_window_heatmap_path <- file.path(
  OUT_DIR,
  paste0("figure_15_class_window_heatmap_dt24_", OUTPUT_PREFIX, ".png")
)
class_window_bar_path <- file.path(
  OUT_DIR,
  paste0("figure_16_class_window_coverage_bar_dt24_", OUTPUT_PREFIX, ".png")
)

p_heat <- ggplot(heat_dt, aes(x = pre_months, y = post_months, fill = N)) +
  geom_tile(color = "white", linewidth = 0.25) +
  geom_text(aes(label = ifelse(N > 0, N, "")), size = 3) +
  scale_x_continuous(breaks = seq(0, 24, by = 2)) +
  scale_y_continuous(breaks = seq(0, 24, by = 2)) +
  scale_fill_gradient(low = "grey95", high = "navy", labels = comma) +
  labs(
    title = "Class-level TSR event-window coverage",
    subtitle = "Each tile shows the number of share classes with a given number of pre- and post-TSR months",
    x = "Months available before TSR month-end",
    y = "Months available after TSR month-end",
    fill = "No. of classes"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

ggsave(filename = class_window_heatmap_path, plot = p_heat, width = 10, height = 8, dpi = 300)

p_bar <- ggplot(coverage_counts_table, aes(x = coverage_group, y = n_classes)) +
  geom_col() +
  geom_text(aes(label = paste0(n_classes, "\n(", round(pct_classes, 1), "%)")),
            vjust = -0.2, size = 4) +
  labs(
    title = "Coverage of TSR event windows across share classes",
    x = NULL,
    y = "Number of classes"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 20, hjust = 1)
  )

ggsave(filename = class_window_bar_path, plot = p_bar, width = 10, height = 6, dpi = 300)

# Standalone table exports. The names intentionally match the workbook sheet
# numbers and table contents so you can quickly identify each output file.
standalone_outputs <- list(
  table_01_core_descriptives = save_table_csv(
    desc_table_core_export,
    paste0("table_01_core_descriptives_", OUTPUT_PREFIX, ".csv")
  ),
  table_02_prepost_class = save_table_csv(
    one_informative_descriptive_table_export,
    paste0("table_02_prepost_by_class_", OUTPUT_PREFIX, ".csv")
  ),
  table_03_prepost_n_mean_median = save_table_csv(
    desc_table_pre_post_with_diffs_export,
    paste0("table_03_prepost_n_mean_median_", OUTPUT_PREFIX, ".csv")
  ),
  table_04_class_structure = save_table_csv(
    class_structure_table,
    paste0("table_04_class_structure_", OUTPUT_PREFIX, ".csv")
  ),
  table_05_tsr_timing_summary = save_table_csv(
    tsr_timing_summary,
    paste0("table_05_tsr_timing_summary_", OUTPUT_PREFIX, ".csv")
  ),
  table_06_tsr_adoption_monthly = save_table_csv(
    tsr_adoption_monthly,
    paste0("table_06_tsr_adoption_monthly_", OUTPUT_PREFIX, ".csv")
  ),
  table_08_pre_retail_vs_inst = save_table_csv(
    pre_retail_vs_inst,
    paste0("table_08_pre_retail_vs_inst_", OUTPUT_PREFIX, ".csv")
  ),
  table_09_manager_concentration = save_table_csv(
    manager_concentration_summary,
    paste0("table_09_manager_concentration_", OUTPUT_PREFIX, ".csv")
  ),
  table_10_top10_managers = save_table_csv(
    top10_managers,
    paste0("table_10_top10_managers_", OUTPUT_PREFIX, ".csv")
  ),
  table_11_quartile_cutoffs = save_table_csv(
    quartile_cutoffs,
    paste0("table_11_quartile_cutoffs_", OUTPUT_PREFIX, ".csv")
  ),
  table_12_fee_quartile_composition = save_table_csv(
    fee_quartile_composition,
    paste0("table_12_fee_quartile_composition_", OUTPUT_PREFIX, ".csv")
  ),
  table_13_window_coverage_counts = save_table_csv(
    coverage_counts_table_export,
    paste0("table_13_window_coverage_counts_dt24_", OUTPUT_PREFIX, ".csv")
  ),
  table_14_window_heatmap_table = save_table_csv(
    pre_post_table,
    paste0("table_14_window_heatmap_matrix_dt24_", OUTPUT_PREFIX, ".csv")
  ),
  table_13a_class_window_summary_raw = save_table_csv(
    class_window_summary,
    paste0("table_13a_class_window_summary_dt24_", OUTPUT_PREFIX, ".csv")
  )
)

# ------------------------------------------------------------------------------
# 9) WRITE ONE EXCEL WORKBOOK: ONE TABLE OR FIGURE PER SHEET
# ------------------------------------------------------------------------------
wb <- createWorkbook()

write_table_sheet(
  wb, "01_Core_Descriptives",
  paste0(
    "Table 2. Descriptive statistics\n",
    "Sample: ", SAMPLE_OBJECT, ". Continuous variables are winsorized at the 1st and 99th percentiles before table construction.\n",
    "Within-FE SD uses fixed effects for ", FE_ID_VAR, " and ", FE_TIME_VAR, ".\n",
    if (USE_DARENDELI_FLOW) {
      "Flow row uses Darendeli-style return-adjusted flow for descriptive reporting."
    } else {
      "Flow row uses the existing flow variable stored in the sample."
    }
  ),
  desc_table_core_export,
  start_row = 5
)

write_table_sheet(
  wb, "02_PrePost_Class",
  paste0(
    "Table. Descriptive statistics by TSR period and class group\n",
    "Panel A reports sample composition. Panel B reports pre- and post-TSR means for the overall sample, retail share classes, and institutional share classes.\n",
    "Continuous variables in Panel B are winsorized at the 1st and 99th percentiles. ",
    if (USE_DARENDELI_FLOW) {
      "Flow is reported using the Darendeli return-adjusted measure."
    } else {
      "Flow is reported using the script-generated flow variable."
    }
  ),
  one_informative_descriptive_table_export,
  start_row = 5
)

write_table_sheet(
  wb, "03_PrePost_N_Mean_Median",
  paste0(
    "Table. Descriptive statistics by TSR period\n",
    "The table reports the number of non-missing observations, means, and medians for the pre-TSR and post-TSR periods in the final event-window sample, together with post-minus-pre differences.\n",
    "Continuous variables are winsorized at the 1st and 99th percentiles. ",
    if (USE_DARENDELI_FLOW) {
      "Flow is reported using the Darendeli return-adjusted measure."
    } else {
      "Flow is reported using the existing script-generated flow variable."
    }
  ),
  desc_table_pre_post_with_diffs_export,
  start_row = 5
)

write_table_sheet(
  wb, "04_Class_Structure",
  "Class structure of the final analysis sample. Counts are based on unique share classes and portfolios in dt_24; retail and institutional classes use clean class classification.",
  class_structure_table
)

write_table_sheet(
  wb, "05_TSR_Timing_Summary",
  "TSR timing summary. Lag is defined as filing date minus report date, in days, using portfolio-level first TSR dates.",
  tsr_timing_summary
)

write_table_sheet(
  wb, "06_TSR_Adoption_Monthly",
  "Monthly counts of first TSR filings and first TSR reports at the portfolio level.",
  tsr_adoption_monthly
)

write_image_sheet(
  wb, "07_TSR_Adoption_Figure",
  "Figure. Monthly first TSR filings and first TSR reports at the portfolio level.",
  tsr_figure_path,
  width = 9,
  height = 5
)

write_table_sheet(
  wb, "08_Pre_Retail_vs_Inst",
  paste0(
    "Pre-period retail versus institutional comparison. The pre period is defined relative to each portfolio's first TSR filing month-end. ",
    "The CAPM and Carhart rows use the ", ALPHA_WINDOW_COMPARE, "-month alpha windows. ",
    "Continuous variables are winsorized at the 1st and 99th percentiles."
  ),
  pre_retail_vs_inst
)

write_table_sheet(
  wb, "09_Manager_Concentration",
  "Management-company concentration summary based on the final sample. Snapshot TNA uses the latest class-level observation in dt_24.",
  manager_concentration_summary
)

write_table_sheet(
  wb, "10_Top10_Managers",
  "Top 10 management companies ranked by share-class-month observations in the final sample.",
  top10_managers
)

write_table_sheet(
  wb, "11_Quartile_Cutoffs",
  "Distribution cutoffs for expense ratio, size, and turnover using the latest class-level snapshot in dt_24 after winsorizing continuous descriptive variables.",
  quartile_cutoffs
)

write_table_sheet(
  wb, "12_Fee_Quartile_Composition",
  "Composition of the sample across expense-ratio quartiles using the latest class-level snapshot in dt_24 after winsorizing continuous descriptive variables.",
  fee_quartile_composition
)

write_table_sheet(
  wb, "13_Window_Coverage_Counts",
  "Class-level TSR event-window coverage counts. This reports how many share classes have full 24-month pre/post windows or incomplete windows on either side of TSR adoption.",
  coverage_counts_table_export
)

write_table_sheet(
  wb, "14_Window_Heatmap_Table",
  "Class-level event-window heatmap matrix. Rows are post-TSR months available and columns are pre-TSR months available; each cell is the number of share classes.",
  pre_post_table
)

write_image_sheet(
  wb, "15_Window_Heatmap_Figure",
  "Figure. Class-level TSR event-window heatmap for dt_24.",
  class_window_heatmap_path,
  width = 10,
  height = 8
)

write_image_sheet(
  wb, "16_Window_Coverage_Bar",
  "Figure. Coverage of TSR event windows across share classes.",
  class_window_bar_path,
  width = 10,
  height = 6
)

xlsx_file <- file.path(OUT_DIR, OUT_WORKBOOK_NAME)
saveWorkbook(wb, xlsx_file, overwrite = TRUE)

# ------------------------------------------------------------------------------
# 10) CONSOLE PREVIEW AND GLOBAL OUTPUT OBJECTS
# ------------------------------------------------------------------------------
cat("\nSaved workbook:\n")
cat("  ", xlsx_file, "\n", sep = "")

cat("\nSaved standalone CSV files:")
for (nm in names(standalone_outputs)) {
  cat("\n  ", nm, ": ", standalone_outputs[[nm]], sep = "")
}
cat("\n\nSaved standalone figure files:")
cat("\n  TSR adoption figure: ", tsr_figure_path, sep = "")
cat("\n  Window heatmap: ", class_window_heatmap_path, sep = "")
cat("\n  Window coverage bar: ", class_window_bar_path, "\n", sep = "")

cat("\nWinsorization applied to continuous descriptive variables using probs = c(",
    paste(WINSOR_PROBS, collapse = ", "), ").\n", sep = "")

cat("\n=== CORE DESCRIPTIVES ===\n")
print(desc_table_core_export)

cat("\n=== PRE/POST BY CLASS ===\n")
print(one_informative_descriptive_table_export)

cat("\n=== PRE/POST N, MEAN, MEDIAN ===\n")
print(desc_table_pre_post_with_diffs_export)

cat("\n=== CLASS STRUCTURE ===\n")
print(class_structure_table)

cat("\n=== TSR TIMING SUMMARY ===\n")
print(tsr_timing_summary)

cat("\n=== PRE-PERIOD RETAIL VS INSTITUTIONAL ===\n")
print(pre_retail_vs_inst)

cat("\n=== MANAGER CONCENTRATION SUMMARY ===\n")
print(manager_concentration_summary)

cat("\n=== FEE QUARTILE COMPOSITION ===\n")
print(fee_quartile_composition)

cat("\n=== WINDOW COVERAGE COUNTS ===\n")
print(coverage_counts_table_export)

cat("\n=== WINDOW HEATMAP MATRIX ===\n")
print(pre_post_table)

assign("desc_table_core", desc_table_core, envir = .GlobalEnv)
assign("desc_table_core_export", desc_table_core_export, envir = .GlobalEnv)
assign("one_informative_descriptive_table_raw", one_informative_descriptive_table_raw, envir = .GlobalEnv)
assign("one_informative_descriptive_table_export", one_informative_descriptive_table_export, envir = .GlobalEnv)
assign("desc_table_pre_post_with_diffs_raw", desc_table_pre_post_with_diffs_raw, envir = .GlobalEnv)
assign("desc_table_pre_post_with_diffs_export", desc_table_pre_post_with_diffs_export, envir = .GlobalEnv)
assign("class_structure_table", class_structure_table, envir = .GlobalEnv)
assign("tsr_timing_summary", tsr_timing_summary, envir = .GlobalEnv)
assign("tsr_adoption_monthly", tsr_adoption_monthly, envir = .GlobalEnv)
assign("pre_retail_vs_inst", pre_retail_vs_inst, envir = .GlobalEnv)
assign("manager_concentration_summary", manager_concentration_summary, envir = .GlobalEnv)
assign("top10_managers", top10_managers, envir = .GlobalEnv)
assign("quartile_cutoffs", quartile_cutoffs, envir = .GlobalEnv)
assign("fee_quartile_composition", fee_quartile_composition, envir = .GlobalEnv)
assign("p_tsr_adoption", p_tsr_adoption, envir = .GlobalEnv)
assign("tsr_figure_path", tsr_figure_path, envir = .GlobalEnv)
assign("class_window_summary", class_window_summary, envir = .GlobalEnv)
assign("coverage_counts_table", coverage_counts_table, envir = .GlobalEnv)
assign("coverage_counts_table_export", coverage_counts_table_export, envir = .GlobalEnv)
assign("class_window_heat_dt", heat_dt, envir = .GlobalEnv)
assign("class_window_heatmap_table", pre_post_table, envir = .GlobalEnv)
assign("p_class_window_heatmap", p_heat, envir = .GlobalEnv)
assign("p_class_window_bar", p_bar, envir = .GlobalEnv)
assign("class_window_heatmap_path", class_window_heatmap_path, envir = .GlobalEnv)
assign("class_window_bar_path", class_window_bar_path, envir = .GlobalEnv)
assign("standalone_outputs", standalone_outputs, envir = .GlobalEnv)
