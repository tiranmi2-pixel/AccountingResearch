# ===================== DESCRIPTIVE PACK: ONE WORKBOOK =====================
# PURPOSE:
#   Build one Excel workbook containing:
#     1) Class-structure table
#     2) TSR timing summary table
#     3) Monthly TSR adoption table
#     4) Monthly TSR adoption figure (filings + reports)
#     5) Pre-period retail vs institutional comparison table
#     6) Management-company concentration summary table
#     7) Top 10 management-company concentration table
#     8) Fee/size/turnover quartile cutoffs table
#     9) Fee-quartile composition table
#
# ASSUMPTION:
#   The full script set has already run and dt_24 exists in memory.
#
# DEFAULT SAMPLE:
#   dt_24 (final event-window sample)
#
# NOTES:
#   - All tables are built from the final analysis sample in memory.
#   - By default, the flow row uses Darendeli-style return-adjusted flow
#     for descriptive reporting.
#   - Retail and institutional classes are "clean" classes:
#       Retail: retail_fund == "Y" and inst_fund != "Y"
#       Institutional: inst_fund == "Y" and retail_fund != "Y"
# ========================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(openxlsx)
})

# ------------------------------------------------------------------------
# 0) USER OPTIONS
# ------------------------------------------------------------------------
SAMPLE_OBJECT <- "dt_24"
OUT_WORKBOOK_NAME <- "descriptive_pack.xlsx"
USE_DARENDELI_FLOW <- TRUE
ALPHA_WINDOW_COMPARE <- 12   # choose 12, 24, or 36 for pre-period class comparison
DIGITS <- 3

# ------------------------------------------------------------------------
# 1) LOAD SAMPLE
# ------------------------------------------------------------------------
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

# ------------------------------------------------------------------------
# 2) HELPERS
# ------------------------------------------------------------------------
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

make_quartile <- function(x) {
  x_num <- suppressWarnings(as.numeric(x))
  out <- rep(NA_integer_, length(x_num))
  ok <- is.finite(x_num)
  if (sum(ok) == 0) return(out)
  
  qs <- quantile(x_num[ok], probs = c(0, .25, .50, .75, 1), na.rm = TRUE, type = 7)
  # ensure strictly increasing breaks when ties exist
  brks <- unique(qs)
  if (length(brks) < 5) {
    # fallback to ntile-like rank grouping
    r <- frank(x_num[ok], ties.method = "average", na.last = "keep")
    out[ok] <- pmin(4L, pmax(1L, ceiling(4 * r / max(r, na.rm = TRUE))))
    return(out)
  }
  out[ok] <- as.integer(cut(x_num[ok], breaks = qs, include.lowest = TRUE, labels = FALSE))
  out
}

write_sheet <- function(wb, sheet_name, title_text, dt_to_write) {
  addWorksheet(wb, sheet_name)
  writeData(wb, sheet_name, title_text, startRow = 1, startCol = 1)
  writeData(wb, sheet_name, dt_to_write, startRow = 4, startCol = 1, colNames = TRUE)
  
  title_style <- createStyle(textDecoration = "bold", fontSize = 12, wrapText = TRUE)
  head_style  <- createStyle(textDecoration = "bold", halign = "center", valign = "center")
  
  addStyle(wb, sheet_name, title_style, rows = 1, cols = 1, gridExpand = TRUE)
  addStyle(wb, sheet_name, head_style, rows = 4, cols = 1:ncol(dt_to_write), gridExpand = TRUE)
  
  setColWidths(wb, sheet_name, cols = 1:ncol(dt_to_write), widths = "auto")
  freezePane(wb, sheet_name, firstActiveRow = 5)
}

# ------------------------------------------------------------------------
# 3) CORE SAMPLE FIELDS
# ------------------------------------------------------------------------
# portfolio-specific post indicator
if (!("tsr_filingdate_first_mend" %in% names(DT))) {
  if ("tsr_filingdate_first" %in% names(DT)) {
    DT[, tsr_filingdate_first := as.Date(tsr_filingdate_first)]
    DT[, tsr_filingdate_first_mend := ceiling_date(tsr_filingdate_first, "month") - days(1)]
  } else {
    stop("Neither tsr_filingdate_first_mend nor tsr_filingdate_first exists in sample.")
  }
}

DT[, post_tsr := as.numeric(!is.na(tsr_filingdate_first_mend) & caldt >= tsr_filingdate_first_mend)]
DT[, period := fifelse(post_tsr == 1, "Post", "Pre")]

# clean class groups
if (!all(c("retail_fund", "inst_fund") %in% names(DT))) {
  stop("retail_fund and/or inst_fund not found in sample.")
}
DT[, retail_num := yn_to_num(retail_fund)]
DT[, inst_num   := yn_to_num(inst_fund)]

DT[, class_group := fifelse(retail_num == 1 & (is.na(inst_num) | inst_num != 1), "Retail",
                            fifelse(inst_num == 1 & (is.na(retail_num) | retail_num != 1), "Institutional",
                                    "Other"))]

# Darendeli-style flow for descriptives
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

# portfolio-level unique timing file from sample
port_dates <- unique(DT[, .(
  crsp_portno,
  tsr_filingdate_first = as.Date(tsr_filingdate_first),
  tsr_reportdate_first = as.Date(tsr_reportdate_first),
  tsr_filingdate_first_mend = as.Date(tsr_filingdate_first_mend),
  pre_months,
  post_months
)])
setorder(port_dates, crsp_portno)

# latest class snapshot for cross-sectional structure tables
class_snapshot <- copy(DT)
setorder(class_snapshot, crsp_fundno, -caldt)
class_snapshot <- class_snapshot[!is.na(crsp_fundno), .SD[1], by = crsp_fundno]

# latest management-company snapshot (class-level)
if (!("mgmt_cd" %in% names(class_snapshot))) {
  stop("mgmt_cd not found in sample.")
}

# ------------------------------------------------------------------------
# 4) TABLE 1: CLASS-STRUCTURE TABLE
# ------------------------------------------------------------------------
# Count clean retail/institutional classes from class snapshot
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

# ------------------------------------------------------------------------
# 5) TABLE 2A: TSR TIMING SUMMARY TABLE
# ------------------------------------------------------------------------
port_dates[, lag_days := as.integer(tsr_filingdate_first - tsr_reportdate_first)]

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

# ------------------------------------------------------------------------
# 6) TABLE 2B: MONTHLY TSR ADOPTION TABLE
# ------------------------------------------------------------------------
port_dates[, filing_monthend := as.Date(ceiling_date(tsr_filingdate_first, "month") - days(1))]
port_dates[, report_monthend := as.Date(ceiling_date(tsr_reportdate_first, "month") - days(1))]

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

# ------------------------------------------------------------------------
# 7) FIGURE: MONTHLY TSR ADOPTION
# ------------------------------------------------------------------------
tsr_figure_path <- file.path(OUT_DIR, "tsr_adoption_timing.png")
figure_created <- FALSE

if (requireNamespace("ggplot2", quietly = TRUE)) {
  suppressPackageStartupMessages(library(ggplot2))
  
  fig_dt <- copy(tsr_adoption_monthly)
  fig_dt[, Month := as.Date(Month)]
  
  fig_long <- rbindlist(list(
    fig_dt[, .(Month, Count = First_TSR_Filings, Series = "First TSR filings")],
    fig_dt[, .(Month, Count = First_TSR_Reports, Series = "First TSR reports")]
  ))
  
  p <- ggplot(fig_long, aes(x = Month, y = Count, color = Series)) +
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
  
  ggsave(filename = tsr_figure_path, plot = p, width = 9, height = 5, dpi = 300)
  figure_created <- TRUE
}

# ------------------------------------------------------------------------
# 8) TABLE 3: PRE-PERIOD RETAIL VS INSTITUTIONAL COMPARISON
# ------------------------------------------------------------------------
capm_var <- paste0("alpha_capm_", ALPHA_WINDOW_COMPARE, "m")
carhart_var <- paste0("alpha_carhart_", ALPHA_WINDOW_COMPARE, "m")

compare_map <- data.table(
  var = c("flow_desc", capm_var, carhart_var,
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

# ------------------------------------------------------------------------
# 9) TABLE 4A: MANAGEMENT-COMPANY CONCENTRATION SUMMARY
# ------------------------------------------------------------------------
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
manager_concentration_summary[, Value := ifelse(Metric == "Management companies",
                                                fmt_int(Value), fmt_num(Value))]

# ------------------------------------------------------------------------
# 10) TABLE 4B: TOP 10 MANAGEMENT COMPANIES
# ------------------------------------------------------------------------
manager_conc[, obs_share_pct := 100 * n_obs / sum(n_obs)]
manager_conc[, tna_share_pct := 100 * snapshot_tna / sum(snapshot_tna)]

top10_managers <- copy(manager_conc[1:min(10, .N),
                                    .(mgmt_cd, mgmt_name, n_share_classes, n_obs, obs_share_pct, snapshot_tna, tna_share_pct)
])

top10_managers[, n_share_classes := fmt_int(n_share_classes)]
top10_managers[, n_obs := fmt_int(n_obs)]
top10_managers[, obs_share_pct := fmt_num(obs_share_pct)]
top10_managers[, snapshot_tna := fmt_num(snapshot_tna)]
top10_managers[, tna_share_pct := fmt_num(tna_share_pct)]

# ------------------------------------------------------------------------
# 11) TABLE 5A: QUARTILE CUTOFFS FOR FEES / SIZE / TURNOVER
# ------------------------------------------------------------------------
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

# ------------------------------------------------------------------------
# 12) TABLE 5B: FEE-QUARTILE COMPOSITION TABLE
# ------------------------------------------------------------------------
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

# ------------------------------------------------------------------------
# 13) WRITE ONE EXCEL WORKBOOK
# ------------------------------------------------------------------------
wb <- createWorkbook()

write_sheet(
  wb, "01_Class_Structure",
  "Class structure of the final analysis sample. Counts are based on unique share classes and portfolios in dt_24; retail and institutional classes use clean class classification.",
  class_structure_table
)

write_sheet(
  wb, "02_TSR_Timing_Summary",
  "TSR timing summary. Lag is defined as filing date minus report date, in days, using portfolio-level first TSR dates.",
  tsr_timing_summary
)

write_sheet(
  wb, "03_TSR_Adoption_Monthly",
  "Monthly counts of first TSR filings and first TSR reports at the portfolio level.",
  tsr_adoption_monthly
)

if (figure_created) {
  addWorksheet(wb, "04_TSR_Timing_Figure")
  writeData(
    wb, "04_TSR_Timing_Figure",
    "Monthly first TSR filings and first TSR reports. The figure is saved separately as a PNG and embedded here.",
    startRow = 1, startCol = 1
  )
  insertImage(
    wb, "04_TSR_Timing_Figure",
    file = tsr_figure_path,
    startRow = 4, startCol = 1,
    width = 9, height = 5, units = "in"
  )
}

write_sheet(
  wb, "05_Pre_Retail_vs_Inst",
  paste0(
    "Pre-period retail versus institutional comparison. The pre period is defined relative to each portfolio's first TSR filing month-end. ",
    "The CAPM and Carhart rows use the ", ALPHA_WINDOW_COMPARE, "-month alpha windows."
  ),
  pre_retail_vs_inst
)

write_sheet(
  wb, "06_Manager_Concentration",
  "Management-company concentration summary based on the final sample. Snapshot TNA uses the latest class-level observation in dt_24.",
  manager_concentration_summary
)

write_sheet(
  wb, "07_Top10_Managers",
  "Top 10 management companies ranked by share-class-month observations in the final sample.",
  top10_managers
)

write_sheet(
  wb, "08_Quartile_Cutoffs",
  "Distribution cutoffs for expense ratio, size, and turnover using the latest class-level snapshot in dt_24.",
  quartile_cutoffs
)

write_sheet(
  wb, "09_Fee_Quartile_Composition",
  "Composition of the sample across expense-ratio quartiles using the latest class-level snapshot in dt_24.",
  fee_quartile_composition
)

xlsx_file <- file.path(OUT_DIR, OUT_WORKBOOK_NAME)
saveWorkbook(wb, xlsx_file, overwrite = TRUE)

# ------------------------------------------------------------------------
# 14) CONSOLE PREVIEW
# ------------------------------------------------------------------------
cat("\nSaved workbook:\n")
cat("  ", xlsx_file, "\n", sep = "")
if (figure_created) {
  cat("Saved figure:\n")
  cat("  ", tsr_figure_path, "\n", sep = "")
}

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

# Keep main outputs in memory
assign("class_structure_table", class_structure_table, envir = .GlobalEnv)
assign("tsr_timing_summary", tsr_timing_summary, envir = .GlobalEnv)
assign("tsr_adoption_monthly", tsr_adoption_monthly, envir = .GlobalEnv)
assign("pre_retail_vs_inst", pre_retail_vs_inst, envir = .GlobalEnv)
assign("manager_concentration_summary", manager_concentration_summary, envir = .GlobalEnv)
assign("top10_managers", top10_managers, envir = .GlobalEnv)
assign("quartile_cutoffs", quartile_cutoffs, envir = .GlobalEnv)
assign("fee_quartile_composition", fee_quartile_composition, envir = .GlobalEnv)