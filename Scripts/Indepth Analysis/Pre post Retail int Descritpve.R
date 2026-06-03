# ===================== ONE INFORMATIVE DESCRIPTIVE TABLE (ROBUST FIX) =====================
# PURPOSE:
#   Create one journal-style descriptive table with:
#     Panel A: Sample composition
#     Panel B: Mean characteristics by period and class
#
# ASSUMPTION:
#   You have already run the full scripts set, so dt_24 exists in memory.
#
# OUTPUT:
#   1) one_informative_descriptive_table.csv
#   2) one_informative_descriptive_table.xlsx
#   3) one_informative_descriptive_table_raw
#   4) one_informative_descriptive_table_export
# ==========================================================================================

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

safe_ttest_p <- function(x_pre, x_post) {
  x_pre  <- suppressWarnings(as.numeric(x_pre))
  x_post <- suppressWarnings(as.numeric(x_post))
  x_pre  <- x_pre[is.finite(x_pre)]
  x_post <- x_post[is.finite(x_post)]
  if (length(x_pre) < 2 || length(x_post) < 2) return(NA_real_)
  tt <- tryCatch(t.test(x_post, x_pre), error = function(e) NULL)
  if (is.null(tt)) return(NA_real_)
  tt$p.value
}

p_to_stars <- function(p) {
  if (is.na(p)) return("")
  if (p < 0.001) return("***")
  if (p < 0.01)  return("**")
  if (p < 0.05)  return("*")
  if (p < 0.10)  return("+")
  ""
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
# 4) CLEAN RETAIL / INSTITUTIONAL GROUPS
# ------------------------------------------------------------------------------
if (!all(c("retail_fund", "inst_fund") %in% names(DT))) {
  stop("retail_fund and/or inst_fund not found in sample.")
}

DT[, retail_num := yn_to_num(retail_fund)]
DT[, inst_num   := yn_to_num(inst_fund)]

DT[, class_group := fifelse(retail_num == 1 & (is.na(inst_num) | inst_num != 1), "Retail",
                            fifelse(inst_num == 1 & (is.na(retail_num) | retail_num != 1), "Institutional",
                                    "Other"))]

# ------------------------------------------------------------------------------
# 5) BUILD FLOW VARIABLE FOR REPORTING
# ------------------------------------------------------------------------------
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
# 6) VARIABLES TO REPORT
# ------------------------------------------------------------------------------
var_specs <- data.table(
  var = c(
    "flow_desc",
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

var_specs <- var_specs[var %in% names(DT)]
if (nrow(var_specs) == 0) stop("None of the requested variables are present.")

# ------------------------------------------------------------------------------
# 7) PANEL A: SAMPLE COMPOSITION (RAW)
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# 8) PANEL B: MEAN CHARACTERISTICS (RAW)
# ------------------------------------------------------------------------------
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
  lapply(seq_len(nrow(var_specs)), function(i) {
    build_mean_row(
      D = DT,
      v = var_specs$var[i],
      lab = var_specs$label[i],
      scl = var_specs$scale[i],
      panel_name = if (i == 1) "Panel B. Mean characteristics by period and class" else ""
    )
  }),
  fill = TRUE
)

# ------------------------------------------------------------------------------
# 9) FORMAT PANEL A SEPARATELY
# ------------------------------------------------------------------------------
panel_A_export <- copy(panel_A_raw)
for (cc in c("Overall_Pre", "Overall_Post", "Overall_Diff",
             "Retail_Pre", "Retail_Post", "Retail_Diff",
             "Inst_Pre", "Inst_Post", "Inst_Diff")) {
  panel_A_export[, (cc) := fmt_int(get(cc))]
}

# ------------------------------------------------------------------------------
# 10) FORMAT PANEL B SEPARATELY
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# 11) COMBINE ONLY AFTER FORMATTING
# ------------------------------------------------------------------------------
one_informative_descriptive_table_export <- rbindlist(
  list(panel_A_export, panel_B_export),
  fill = TRUE
)

# raw combined version too
one_informative_descriptive_table_raw <- rbindlist(
  list(panel_A_raw, panel_B_raw),
  fill = TRUE
)

# Keep final display columns only
keep_cols <- c("Panel", "Variable",
               "Overall_Pre", "Overall_Post", "Overall_Diff",
               "Retail_Pre", "Retail_Post", "Retail_Diff",
               "Inst_Pre", "Inst_Post", "Inst_Diff")

one_informative_descriptive_table_export <- one_informative_descriptive_table_export[, ..keep_cols]

# ------------------------------------------------------------------------------
# 12) SAVE CSV
# ------------------------------------------------------------------------------
csv_file <- file.path(OUT_DIR, "one_informative_descriptive_table.csv")
fwrite(one_informative_descriptive_table_export, csv_file)

# ------------------------------------------------------------------------------
# 13) SAVE EXCEL
# ------------------------------------------------------------------------------
xlsx_file <- file.path(OUT_DIR, "one_informative_descriptive_table.xlsx")

wb <- createWorkbook()
addWorksheet(wb, "Table")

title_text <- paste0(
  "Table. Descriptive statistics by TSR period and class group\n",
  "Panel A reports sample composition. Panel B reports pre- and post-TSR means for the overall sample, retail share classes, and institutional share classes.\n",
  if (USE_DARENDELI_FLOW) {
    "Flow is reported using the Darendeli return-adjusted measure."
  } else {
    "Flow is reported using the script-generated flow variable."
  }
)

writeData(wb, "Table", title_text, startRow = 1, startCol = 1)
writeData(wb, "Table", one_informative_descriptive_table_export, startRow = 5, startCol = 1, colNames = TRUE)

title_style <- createStyle(textDecoration = "bold", fontSize = 12, wrapText = TRUE)
head_style  <- createStyle(textDecoration = "bold", halign = "center", valign = "center")
panel_style <- createStyle(textDecoration = "bold")

addStyle(wb, "Table", title_style, rows = 1, cols = 1, gridExpand = TRUE)
addStyle(wb, "Table", head_style, rows = 5, cols = 1:ncol(one_informative_descriptive_table_export), gridExpand = TRUE)

panel_rows <- which(one_informative_descriptive_table_export$Panel != "") + 5
if (length(panel_rows) > 0) {
  addStyle(wb, "Table", panel_style, rows = panel_rows, cols = 1, gridExpand = TRUE)
}

setColWidths(wb, "Table", cols = 1:ncol(one_informative_descriptive_table_export), widths = "auto")
freezePane(wb, "Table", firstActiveRow = 6)
saveWorkbook(wb, xlsx_file, overwrite = TRUE)

# ------------------------------------------------------------------------------
# 14) PRINT PREVIEW
# ------------------------------------------------------------------------------
cat("\nSaved files:\n")
cat("  CSV : ", csv_file, "\n", sep = "")
cat("  XLSX: ", xlsx_file, "\n\n", sep = "")

cat("=== PANEL A RAW ===\n")
print(panel_A_raw)

cat("\n=== PANEL A EXPORT ===\n")
print(panel_A_export)

cat("\n=== FINAL EXPORT TABLE ===\n")
print(one_informative_descriptive_table_export)

assign("one_informative_descriptive_table_raw", one_informative_descriptive_table_raw, envir = .GlobalEnv)
assign("one_informative_descriptive_table_export", one_informative_descriptive_table_export, envir = .GlobalEnv)