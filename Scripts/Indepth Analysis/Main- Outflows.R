# ===================== PROFESSOR TABLE: alpha_capm_12m (OUTFLOW t+1, t REGRESSORS) =====================
# PURPOSE:
#   3-column professor table:
#     (1) Combined classes
#     (2) Model 1 - Retail
#     (3) Model 1 - Institutional
#
# IMPORTANT:
#   - Core logic of the remaining models is unchanged
#   - Model 2 / DiD has been removed as requested
#   - Dependent variable = outflow at t+1
#   - Performance variable = alpha_capm_12m at t
#   - Controls are measured at t
#   - All models use CLASS-LEVEL fixed effects: crsp_fundno + ym
#   - Formatting/export only has been upgraded

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(lubridate)
  library(modelsummary)
  library(openxlsx)
  library(kableExtra)
})

winsorize_vec <- function(x, probs = c(0.01, 0.99)) {
  if (!is.numeric(x)) return(x)
  qs <- quantile(x, probs = probs, na.rm = TRUE, type = 7)
  if (!is.finite(qs[1]) || !is.finite(qs[2])) return(x)
  x[x < qs[1]] <- qs[1]
  x[x > qs[2]] <- qs[2]
  x
}

winsorize_dt <- function(DT, vars, probs = c(0.01, 0.99)) {
  out <- copy(DT)
  vars <- intersect(vars, names(out))
  for (v in vars) {
    suppressWarnings(out[, (v) := winsorize_vec(get(v), probs = probs)])
  }
  out
}

clean_modelsummary_table <- function(x) {
  x <- as.data.frame(x, stringsAsFactors = FALSE)
  
  nm <- names(x)
  part_col <- if ("part" %in% nm) "part" else NULL
  term_col <- if ("term" %in% nm) "term" else nm[1]
  stat_col <- if ("statistic" %in% nm) "statistic" else NULL
  model_cols <- setdiff(nm, c(part_col, term_col, stat_col))
  
  out <- data.frame(
    term = x[[term_col]],
    x[, model_cols, drop = FALSE],
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  
  if (!is.null(stat_col)) {
    stat_vals <- trimws(as.character(x[[stat_col]]))
    out[stat_vals != "" & stat_vals != "estimate", 1] <- ""
  }
  
  names(out)[1] <- ""
  
  out[] <- lapply(out, function(z) {
    if (is.character(z)) trimws(z) else z
  })
  
  rownames(out) <- NULL
  out
}

blank_like <- function(x) {
  y <- x[0, , drop = FALSE]
  y[1, ] <- ""
  y
}

# -----------------------
# 0) Setup
# -----------------------
stopifnot(exists("dt_24", envir = .GlobalEnv))
dt0 <- as.data.table(copy(get("dt_24", envir = .GlobalEnv)))

stopifnot(exists("proj_root", envir = .GlobalEnv))

need_cols <- c(
  "crsp_fundno","crsp_portno","caldt","mtna","mret",
  "retail_fund","inst_fund","tsr_filingdate_first_mend",
  "alpha_capm_12m","first_offer_dt","mgmt_cd"
)
stopifnot(all(need_cols %in% names(dt0)))

dt0[, caldt := as.Date(caldt)]
dt0[, ym := format(caldt, "%Y-%m")]
dt0[, first_offer_dt := as.Date(first_offer_dt)]
dt0[, crsp_fundno := as.numeric(crsp_fundno)]
dt0[, crsp_portno := as.numeric(crsp_portno)]
dt0[, mgmt_cd := as.numeric(mgmt_cd)]
dt0[, mtna := as.numeric(mtna)]
dt0[, mret := as.numeric(mret)]

if (is.finite(mean(abs(dt0$mret), na.rm = TRUE)) &&
    mean(abs(dt0$mret), na.rm = TRUE) > 1) {
  dt0[, mret := mret / 100]
}

# -----------------------
# 1) Class flags
# -----------------------
dt0[, retail := as.integer(retail_fund == "Y" & (is.na(inst_fund) | inst_fund != "Y"))]
dt0[, inst   := as.integer(inst_fund == "Y")]

# -----------------------
# 2) Build local contemporaneous controls + Darendeli flow
#    These are LOCAL to this script only
# -----------------------
setorder(dt0, crsp_fundno, caldt)

dt0[, age_years_t := as.numeric(caldt - first_offer_dt) / 365.25]
dt0[, log_tna_t   := log1p(mtna)]

dt0[, family_tna_t := sum(mtna, na.rm = TRUE), by = .(mgmt_cd, caldt)]
dt0[, log_familytna_t := fifelse(
  is.na(family_tna_t) | family_tna_t <= 0,
  NA_real_,
  log(family_tna_t)
)]

if ("turn_ratio" %in% names(dt0)) {
  dt0[, turn_ratio_t := as.numeric(turn_ratio)]
} else {
  dt0[, turn_ratio_t := NA_real_]
}
if ("mgmt_fee" %in% names(dt0)) {
  dt0[, mgmt_fee_t := as.numeric(mgmt_fee)]
} else {
  dt0[, mgmt_fee_t := NA_real_]
}
if ("exp_ratio" %in% names(dt0)) {
  dt0[, exp_ratio_t := as.numeric(exp_ratio)]
} else {
  dt0[, exp_ratio_t := NA_real_]
}

dt0[, mtna_l1_local := shift(mtna, 1L), by = crsp_fundno]
dt0[, flow_dar_t := fifelse(
  is.na(mtna_l1_local) | mtna_l1_local <= 0 | is.na(mret) | (1 + mret) <= 0,
  NA_real_,
  (mtna - mtna_l1_local * (1 + mret)) / (mtna_l1_local * (1 + mret))
)]

# Outflow at t = negative part of Darendeli flow, made positive
dt0[, outflow_t := pmax(-flow_dar_t, 0)]

# Outflow at t+1 = dependent variable
dt0[, outflow_tp1 := shift(outflow_t, 1L, type = "lead"), by = crsp_fundno]

# Performance at t
dt0[, perf_t := as.numeric(alpha_capm_12m)]

# -----------------------
# 3) Helper: build fund-specific post at portno level
# -----------------------
add_post_from_adoption <- function(DT) {
  out <- copy(DT)
  out[, adopt_mend_raw := as.Date(tsr_filingdate_first_mend)]
  out[, adopt_mend := suppressWarnings(min(adopt_mend_raw, na.rm = TRUE)), by = crsp_portno]
  out[!is.finite(adopt_mend), adopt_mend := as.Date(NA)]
  out <- out[!is.na(adopt_mend)]
  out[, post_fund := as.integer(caldt >= adopt_mend)]
  out
}

controls_m1_needed <- c(
  "age_years_t",
  "log_tna_t",
  "flow_dar_t",
  "log_familytna_t",
  "turn_ratio_t",
  "mgmt_fee_t",
  "exp_ratio_t"
)

# -----------------------
# 4) Samples
# -----------------------

# Retail
dtR <- dt0[retail == 1 & inst == 0]
dtR <- add_post_from_adoption(dtR)
controls_m1_R <- controls_m1_needed[controls_m1_needed %in% names(dtR)]
rhs_controls_m1_R <- if (length(controls_m1_R) > 0) paste(controls_m1_R, collapse = " + ") else "0"

# Institutional
dtI <- dt0[inst == 1]
dtI <- add_post_from_adoption(dtI)
controls_m1_I <- controls_m1_needed[controls_m1_needed %in% names(dtI)]
rhs_controls_m1_I <- if (length(controls_m1_I) > 0) paste(controls_m1_I, collapse = " + ") else "0"

# Combined classes
dtC <- dt0[(retail == 1 & inst == 0) | (inst == 1 & retail == 0)]
dtC[, retail := as.integer(retail == 1)]
dtC <- add_post_from_adoption(dtC)
controls_m1_C <- controls_m1_needed[controls_m1_needed %in% names(dtC)]
rhs_controls_m1_C <- if (length(controls_m1_C) > 0) paste(controls_m1_C, collapse = " + ") else "0"

# -----------------------
# 5) Winsorize full dataset BEFORE splitting
#    Same timing logic as original script
# -----------------------
winsor_vars <- c(
  "outflow_tp1",
  "perf_t",
  "age_years_t",
  "log_tna_t",
  "flow_dar_t",
  "log_familytna_t",
  "turn_ratio_t",
  "mgmt_fee_t",
  "exp_ratio_t"
)

dt0_win <- winsorize_dt(dt0, winsor_vars, probs = c(0.01, 0.99))

dtR_win <- dt0_win[retail == 1 & inst == 0]
dtR_win <- add_post_from_adoption(dtR_win)

dtI_win <- dt0_win[inst == 1]
dtI_win <- add_post_from_adoption(dtI_win)

dtC_win <- dt0_win[(retail == 1 & inst == 0) | (inst == 1 & retail == 0)]
dtC_win[, retail := as.integer(retail == 1)]
dtC_win <- add_post_from_adoption(dtC_win)

run_professor_table <- function(dtR_in, dtI_in, dtC_in, out_subdir) {
  dtR <- copy(dtR_in)
  dtI <- copy(dtI_in)
  dtC <- copy(dtC_in)
  
  OUT_DIR_RUN <- file.path(
    get("proj_root", envir = .GlobalEnv),
    "Results",
    "professor_alpha_capm_12m_outflow_tplus1_classFE",
    out_subdir
  )
  dir.create(OUT_DIR_RUN, recursive = TRUE, showWarnings = FALSE)
  
  # -----------------------
  # 6) Missing-value handling
  # -----------------------
  
  # Combined
  if (length(controls_m1_C) > 0) {
    for (cc in controls_m1_C) {
      suppressWarnings(dtC[, (cc) := as.numeric(get(cc))])
      dtC[is.na(get(cc)), (cc) := 0]
    }
  }
  core_keep_C <- c("outflow_tp1","perf_t","post_fund","retail","crsp_fundno","crsp_portno","ym")
  dtC <- dtC[complete.cases(dtC[, ..core_keep_C])]
  
  # Retail
  if (length(controls_m1_R) > 0) {
    for (cc in controls_m1_R) {
      suppressWarnings(dtR[, (cc) := as.numeric(get(cc))])
      dtR[is.na(get(cc)), (cc) := 0]
    }
  }
  core_keep_R <- c("outflow_tp1","perf_t","post_fund","crsp_fundno","crsp_portno","ym")
  dtR <- dtR[complete.cases(dtR[, ..core_keep_R])]
  
  # Institutional
  if (length(controls_m1_I) > 0) {
    for (cc in controls_m1_I) {
      suppressWarnings(dtI[, (cc) := as.numeric(get(cc))])
      dtI[is.na(get(cc)), (cc) := 0]
    }
  }
  core_keep_I <- c("outflow_tp1","perf_t","post_fund","crsp_fundno","crsp_portno","ym")
  dtI <- dtI[complete.cases(dtI[, ..core_keep_I])]
  
  # -----------------------
  # 7) Estimate models
  # -----------------------
  
  # Combined classes
  fml_C <- as.formula(paste0(
    "outflow_tp1 ~ perf_t * post_fund + retail + ",
    if (rhs_controls_m1_C != "0") rhs_controls_m1_C else "0",
    " | crsp_fundno + ym"
  ))
  m_combined <- feols(fml_C, data = dtC, cluster = ~ crsp_portno)
  
  # Model 1 - Retail
  fml_R <- as.formula(paste0(
    "outflow_tp1 ~ perf_t * post_fund + ",
    if (rhs_controls_m1_R != "0") rhs_controls_m1_R else "0",
    " | crsp_fundno + ym"
  ))
  m_retail <- feols(fml_R, data = dtR, cluster = ~ crsp_portno)
  
  # Model 1 - Institutional
  fml_I <- as.formula(paste0(
    "outflow_tp1 ~ perf_t * post_fund + ",
    if (rhs_controls_m1_I != "0") rhs_controls_m1_I else "0",
    " | crsp_fundno + ym"
  ))
  m_inst <- feols(fml_I, data = dtI, cluster = ~ crsp_portno)
  
  # -----------------------
  # 8) Output maps and labels
  # -----------------------
  models <- list(
    "Combined classes"        = m_combined,
    "Model 1 - Retail"        = m_retail,
    "Model 1 - Institutional" = m_inst
  )
  
  coef_map <- c(
    "perf_t"           = "Perf (t)",
    "post_fund"        = "Post (fund TSR adoption)",
    "perf_t:post_fund" = "Perf (t) × Post",
    "retail"           = "Retail indicator",
    "age_years_t"      = "Fund age (t)",
    "log_tna_t"        = "Log(TNA) (t)",
    "flow_dar_t"       = "Darendeli flow (t)",
    "log_familytna_t"  = "Log(Family TNA) (t)",
    "turn_ratio_t"     = "Turnover (t)",
    "mgmt_fee_t"       = "Management fee (t)",
    "exp_ratio_t"      = "Expense ratio (t)"
  )
  
  gof_map <- data.frame(
    raw = c("nobs"),
    clean = c("N"),
    fmt = c(0),
    stringsAsFactors = FALSE
  )
  
  add_rows <- data.frame(
    term = c(
      "Class Fixed Effects",
      "Year-Month Fixed Effects",
      "Clustered by",
      "Dependent variable",
      "R-squared",
      "Adjusted R-squared",
      "Within R-squared",
      "Adjusted Within R-squared"
    ),
    "Combined classes" = c(
      "Yes",
      "Yes",
      "crsp_portno",
      "Outflow (t+1)",
      sprintf("%.3f", fitstat(m_combined, "r2")),
      sprintf("%.3f", fitstat(m_combined, "ar2")),
      sprintf("%.3f", fitstat(m_combined, "wr2")),
      sprintf("%.3f", fitstat(m_combined, "war2"))
    ),
    "Model 1 - Retail" = c(
      "Yes",
      "Yes",
      "crsp_portno",
      "Outflow (t+1)",
      sprintf("%.3f", fitstat(m_retail, "r2")),
      sprintf("%.3f", fitstat(m_retail, "ar2")),
      sprintf("%.3f", fitstat(m_retail, "wr2")),
      sprintf("%.3f", fitstat(m_retail, "war2"))
    ),
    "Model 1 - Institutional" = c(
      "Yes",
      "Yes",
      "crsp_portno",
      "Outflow (t+1)",
      sprintf("%.3f", fitstat(m_inst, "r2")),
      sprintf("%.3f", fitstat(m_inst, "ar2")),
      sprintf("%.3f", fitstat(m_inst, "wr2")),
      sprintf("%.3f", fitstat(m_inst, "war2"))
    ),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  
  notes_txt <- paste0(
    "Dependent variable: outflow at t+1. ",
    "Darendeli flow is defined locally in this script as (TNA_t - TNA_{t-1}(1+R_t)) / (TNA_{t-1}(1+R_t)). ",
    "Outflow is defined as the negative part of Darendeli flow, made positive, i.e. max(-Darendeli flow, 0), ",
    "and the regression outcome uses its one-month lead. ",
    "Performance variable: alpha_capm_12m at t. ",
    "Controls are measured at t. ",
    "All models use class fixed effects (crsp_fundno) plus year-month fixed effects. ",
    "t statistics are shown in parentheses. Standard errors are clustered by crsp_portno. ",
    "This script does not modify dt_24 or any saved pipeline data."
  )
  
  # -----------------------
  # 9) Build clean export table
  # -----------------------
  tab_raw <- modelsummary(
    models,
    coef_map = coef_map,
    coef_omit = NULL,
    gof_map = gof_map,
    add_rows = add_rows,
    stars = TRUE,
    estimate = "{estimate}{stars}",
    statistic = "({statistic})",
    output = "data.frame"
  )
  
  tab_df <- clean_modelsummary_table(tab_raw)
  model_labels <- names(tab_df)[-1]
  n_models <- length(model_labels)
  
  # -----------------------
  # 10) HTML export
  # -----------------------
  html_df <- tab_df
  colnames(html_df) <- c("", model_labels)
  
  html_table <- kbl(
    html_df,
    format = "html",
    escape = FALSE,
    align = c("l", rep("c", n_models)),
    col.names = c("", model_labels),
    booktabs = TRUE,
    linesep = ""
  ) |>
    add_header_above(
      setNames(c(1, rep(1, n_models)), c(" ", paste0("(", seq_len(n_models), ")"))),
      bold = FALSE,
      align = "center"
    ) |>
    kable_styling(
      full_width = FALSE,
      position = "center",
      font_size = 16,
      html_font = "Times New Roman"
    ) |>
    row_spec(0, extra_css = "border-top: 2px solid black; border-bottom: 1px solid black;") |>
    column_spec(1, width = "28em") |>
    column_spec(2:(n_models + 1), width = "14em")
  
  html_title <- paste0(
    "<div style='width:100%; max-width:1100px; margin:0 auto; font-family:\"Times New Roman\", serif;'>",
    "<div style='font-size:20px; margin-bottom:10px;'>",
    "<span style='font-weight:bold;'>Table 5</span>",
    "<span style='margin-left:12px;'>Determinants analysis</span>",
    "</div>",
    "</div>"
  )
  
  html_notes <- paste0(
    "<div style='width:100%; max-width:1100px; margin:12px auto 0 auto;",
    " font-family:\"Times New Roman\", serif; font-size:15px; line-height:1.4;'>",
    "<em>Notes:</em> ", notes_txt,
    "</div>"
  )
  
  writeLines(
    c(
      "<html><head><meta charset='UTF-8'></head><body>",
      html_title,
      "<div style='width:100%; max-width:1100px; margin:0 auto;'>",
      as.character(html_table),
      "</div>",
      html_notes,
      "</body></html>"
    ),
    con = file.path(OUT_DIR_RUN, "professor_table_alpha_capm_12m_outflow_tplus1_classFE.html")
  )
  
  # -----------------------
  # 11) TeX export
  # -----------------------
  modelsummary(
    models,
    coef_map = coef_map,
    coef_omit = NULL,
    gof_map = gof_map,
    add_rows = add_rows,
    stars = TRUE,
    estimate = "{estimate}{stars}",
    statistic = "({statistic})",
    notes = notes_txt,
    title = "Table 5 Determinants analysis",
    output = file.path(OUT_DIR_RUN, "professor_table_alpha_capm_12m_outflow_tplus1_classFE.tex")
  )
  
  # -----------------------
  # 12) CSV export
  # -----------------------
  csv_title <- blank_like(tab_df)
  csv_title[1, 1] <- "Table 5  Determinants analysis"
  
  csv_numrow <- blank_like(tab_df)
  csv_numrow[1, 2:(n_models + 1)] <- paste0("(", seq_len(n_models), ")")
  
  csv_labrow <- blank_like(tab_df)
  csv_labrow[1, 2:(n_models + 1)] <- model_labels
  
  csv_out <- rbind(csv_title, csv_numrow, csv_labrow, tab_df)
  
  fwrite(
    csv_out,
    file = file.path(OUT_DIR_RUN, "professor_table_alpha_capm_12m_outflow_tplus1_classFE.csv")
  )
  
  # -----------------------
  # 13) Excel export
  # -----------------------
  wb <- createWorkbook()
  addWorksheet(wb, "Regression Table", gridLines = FALSE)
  
  sheet <- "Regression Table"
  
  title_style <- createStyle(
    fontName = "Times New Roman",
    fontSize = 14,
    textDecoration = "bold",
    halign = "left",
    border = "bottom",
    borderStyle = "thick"
  )
  
  num_style <- createStyle(
    fontName = "Times New Roman",
    fontSize = 12,
    halign = "center"
  )
  
  label_style <- createStyle(
    fontName = "Times New Roman",
    fontSize = 12,
    halign = "center",
    textDecoration = "italic",
    border = "bottom",
    borderStyle = "medium"
  )
  
  body_left_style <- createStyle(
    fontName = "Times New Roman",
    fontSize = 12,
    halign = "left"
  )
  
  body_center_style <- createStyle(
    fontName = "Times New Roman",
    fontSize = 12,
    halign = "center"
  )
  
  stat_style <- createStyle(
    fontName = "Times New Roman",
    fontSize = 11,
    halign = "center",
    textDecoration = "italic"
  )
  
  summary_top_style <- createStyle(
    fontName = "Times New Roman",
    fontSize = 12,
    border = "top",
    borderStyle = "medium"
  )
  
  notes_style <- createStyle(
    fontName = "Times New Roman",
    fontSize = 11,
    textDecoration = "italic",
    wrapText = TRUE,
    valign = "top"
  )
  
  writeData(wb, sheet, x = "Table 5  Determinants analysis", startRow = 1, startCol = 1, colNames = FALSE)
  mergeCells(wb, sheet, cols = 1:(n_models + 1), rows = 1)
  addStyle(wb, sheet, title_style, rows = 1, cols = 1:(n_models + 1), gridExpand = TRUE)
  
  for (j in seq_len(n_models)) {
    writeData(wb, sheet, x = paste0("(", j, ")"), startRow = 3, startCol = j + 1, colNames = FALSE)
  }
  addStyle(wb, sheet, num_style, rows = 3, cols = 1:(n_models + 1), gridExpand = TRUE)
  
  for (j in seq_len(n_models)) {
    writeData(wb, sheet, x = model_labels[j], startRow = 4, startCol = j + 1, colNames = FALSE)
  }
  addStyle(wb, sheet, label_style, rows = 4, cols = 1:(n_models + 1), gridExpand = TRUE)
  
  writeData(wb, sheet, x = tab_df, startRow = 6, startCol = 1, colNames = FALSE)
  n_body <- nrow(tab_df)
  
  addStyle(wb, sheet, body_left_style, rows = 6:(5 + n_body), cols = 1, gridExpand = TRUE)
  addStyle(wb, sheet, body_center_style, rows = 6:(5 + n_body), cols = 2:(n_models + 1), gridExpand = TRUE)
  
  stat_rows <- which(tab_df[[1]] == "") + 5
  if (length(stat_rows) > 0) {
    addStyle(
      wb, sheet, stat_style,
      rows = stat_rows, cols = 2:(n_models + 1),
      gridExpand = TRUE, stack = TRUE
    )
  }
  
  sum_start <- which(tab_df[[1]] %in% c("N", "Observations"))[1]
  if (!is.na(sum_start)) {
    excel_row <- sum_start + 5
    addStyle(
      wb, sheet, summary_top_style,
      rows = excel_row, cols = 1:(n_models + 1),
      gridExpand = TRUE, stack = TRUE
    )
  }
  
  setColWidths(wb, sheet, cols = 1, widths = 34)
  setColWidths(wb, sheet, cols = 2:(n_models + 1), widths = 20)
  
  note_row <- 8 + n_body
  writeData(wb, sheet, x = "Notes:", startRow = note_row, startCol = 1, colNames = FALSE)
  writeData(wb, sheet, x = notes_txt, startRow = note_row + 1, startCol = 1, colNames = FALSE)
  mergeCells(wb, sheet, cols = 1:(n_models + 1), rows = note_row + 1)
  addStyle(wb, sheet, notes_style, rows = note_row:(note_row + 1), cols = 1:(n_models + 1), gridExpand = TRUE)
  
  addWorksheet(wb, "Notes", gridLines = FALSE)
  writeData(
    wb,
    "Notes",
    data.frame(Notes = notes_txt, stringsAsFactors = FALSE)
  )
  setColWidths(wb, "Notes", cols = 1, widths = 120)
  
  addWorksheet(wb, "Sample Sizes", gridLines = FALSE)
  writeData(
    wb,
    "Sample Sizes",
    data.frame(
      Model = model_labels,
      N = sapply(models, nobs),
      check.names = FALSE,
      stringsAsFactors = FALSE
    )
  )
  setColWidths(wb, "Sample Sizes", cols = 1:2, widths = c(28, 12))
  
  saveWorkbook(
    wb,
    file.path(OUT_DIR_RUN, "professor_table_alpha_capm_12m_outflow_tplus1_classFE.xlsx"),
    overwrite = TRUE
  )
  
  cat("\n===================== PROFESSOR TABLE BUILT =====================\n")
  cat("Output folder:\n", OUT_DIR_RUN, "\n\n")
  cat("Files created:\n")
  cat(" - professor_table_alpha_capm_12m_outflow_tplus1_classFE.html\n")
  cat(" - professor_table_alpha_capm_12m_outflow_tplus1_classFE.tex\n")
  cat(" - professor_table_alpha_capm_12m_outflow_tplus1_classFE.xlsx\n")
  cat(" - professor_table_alpha_capm_12m_outflow_tplus1_classFE.csv\n\n")
  
  cat("Sample sizes:\n")
  cat("Combined classes:        ", nobs(m_combined), "\n")
  cat("Model 1 - Retail:        ", nobs(m_retail), "\n")
  cat("Model 1 - Institutional: ", nobs(m_inst), "\n")
}

# -----------------------
# 14) Run both versions
# -----------------------
run_professor_table(
  dtR_in = dtR,
  dtI_in = dtI,
  dtC_in = dtC,
  out_subdir = "without_winsorization"
)

run_professor_table(
  dtR_in = dtR_win,
  dtI_in = dtI_win,
  dtC_in = dtC_win,
  out_subdir = "with_winsorization"
)