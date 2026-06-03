# ===================== PROFESSOR TABLE: alpha_capm_12m (DAR + T+1 FLOW, T REGRESSORS) =====================
# PURPOSE:
#   Same 4-column structure as before:
#     (1) Model 1 - Retail
#     (2) Model 1 - Institutional
#     (3) Combined classes
#     (4) DiD - Model 2
#
# CHANGES IN THIS VERSION:
#   - Dependent variable = Darendeli flow at t+1
#   - Performance variable = alpha_capm_12m at t
#   - Control variables = measured at t
#   - All models now use CLASS-LEVEL fixed effects: crsp_fundno + ym
#   - All changes are local to this script only; dt_24 is not modified

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
  out[] <- lapply(out, function(z) if (is.character(z)) trimws(z) else z)
  rownames(out) <- NULL
  out
}

fmt_num <- function(x, digits = 3) {
  ifelse(is.na(x), "", sprintf(paste0("%.", digits, "f"), x))
}

make_test_row <- function(term, idx, value, cn) {
  vals <- rep("", length(cn))
  vals[idx] <- value
  out <- data.frame(term = term, stringsAsFactors = FALSE)
  for (j in seq_along(cn)) out[[cn[j]]] <- vals[j]
  out
}

extract_term_est_se <- function(model, parts) {
  ct <- as.data.frame(summary(model)$coeftable)
  rn <- rownames(ct)

  idx <- which(vapply(
    strsplit(rn, ":", fixed = TRUE),
    function(v) all(parts %in% v) && length(v) == length(parts),
    logical(1)
  ))

  if (length(idx) == 0) {
    return(list(term = NA_character_, beta = NA_real_, se = NA_real_))
  }

  term <- rn[idx[1]]
  list(
    term = term,
    beta = unname(ct[term, 1]),
    se   = unname(ct[term, 2])
  )
}

compute_z_from_models <- function(model_q3, model_q1, parts) {
  q3 <- extract_term_est_se(model_q3, parts = parts)
  q1 <- extract_term_est_se(model_q1, parts = parts)
  
  diff <- q3$beta - q1$beta
  zval <- diff / sqrt(q3$se^2 + q1$se^2)
  pval <- 2 * pnorm(-abs(zval))
  
  list(
    beta_q3 = q3$beta,
    se_q3   = q3$se,
    beta_q1 = q1$beta,
    se_q1   = q1$se,
    diff    = diff,
    z       = zval,
    p       = pval
  )
}

export_formatted_table <- function(models,
                                   coef_map,
                                   gof_map,
                                   add_rows = NULL,
                                   notes_txt,
                                   title_text,
                                   display_labels,
                                   html_path,
                                   tex_path,
                                   xlsx_path) {
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
  n_models <- length(display_labels)

  html_df <- tab_df
  colnames(html_df) <- c("", display_labels)

  html_table <- kbl(
    html_df,
    format = "html",
    escape = FALSE,
    align = c("l", rep("c", n_models)),
    col.names = c("", display_labels),
    booktabs = TRUE,
    linesep = ""
  ) |>
    add_header_above(
      c(" " = 1, stats::setNames(rep(1, n_models), paste0("(", seq_len(n_models), ")"))),
      bold = FALSE,
      align = "center"
    ) |>
    kable_styling(
      full_width = FALSE,
      position = "center",
      font_size = 15,
      html_font = "Times New Roman"
    ) |>
    row_spec(0, extra_css = "border-top: 2px solid black; border-bottom: 1px solid black;") |>
    column_spec(1, width = "30em") |>
    column_spec(2:(n_models + 1), width = "13em")

  html_title <- paste0(
    "<div style='width:100%; max-width:1450px; margin:0 auto; font-family:\"Times New Roman\", serif;'>",
    "<div style='font-size:20px; margin-bottom:10px;'>",
    "<span style='font-weight:bold;'>", title_text, "</span>",
    "</div></div>"
  )

  html_notes <- paste0(
    "<div style='width:100%; max-width:1450px; margin:12px auto 0 auto;",
    " font-family:\"Times New Roman\", serif; font-size:15px; line-height:1.4;'>",
    "<em>Notes:</em> ", notes_txt,
    "</div>"
  )

  writeLines(
    c(
      "<html><head><meta charset='UTF-8'></head><body>",
      html_title,
      "<div style='width:100%; max-width:1450px; margin:0 auto;'>",
      as.character(html_table),
      "</div>",
      html_notes,
      "</body></html>"
    ),
    con = html_path
  )

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
    title = title_text,
    output = tex_path
  )

  wb <- createWorkbook()
  addWorksheet(wb, "Regression Table", gridLines = FALSE)
  sheet <- "Regression Table"

  title_style <- createStyle(
    fontName = "Times New Roman", fontSize = 14,
    textDecoration = "bold", halign = "left",
    border = "bottom", borderStyle = "thick"
  )
  num_style <- createStyle(
    fontName = "Times New Roman", fontSize = 12, halign = "center"
  )
  label_style <- createStyle(
    fontName = "Times New Roman", fontSize = 12,
    halign = "center", textDecoration = "italic",
    border = "bottom", borderStyle = "medium"
  )
  body_left_style <- createStyle(
    fontName = "Times New Roman", fontSize = 12, halign = "left"
  )
  body_center_style <- createStyle(
    fontName = "Times New Roman", fontSize = 12, halign = "center"
  )
  stat_style <- createStyle(
    fontName = "Times New Roman", fontSize = 11,
    halign = "center", textDecoration = "italic"
  )
  summary_top_style <- createStyle(
    fontName = "Times New Roman", fontSize = 12,
    border = "top", borderStyle = "medium"
  )
  notes_style <- createStyle(
    fontName = "Times New Roman", fontSize = 11,
    textDecoration = "italic", wrapText = TRUE, valign = "top"
  )

  writeData(wb, sheet, x = title_text, startRow = 1, startCol = 1, colNames = FALSE)
  mergeCells(wb, sheet, cols = 1:(n_models + 1), rows = 1)
  addStyle(wb, sheet, title_style, rows = 1, cols = 1:(n_models + 1), gridExpand = TRUE)

  for (j in seq_len(n_models)) {
    writeData(wb, sheet, x = paste0("(", j, ")"), startRow = 3, startCol = j + 1, colNames = FALSE)
  }
  addStyle(wb, sheet, num_style, rows = 3, cols = 1:(n_models + 1), gridExpand = TRUE)

  for (j in seq_len(n_models)) {
    writeData(wb, sheet, x = display_labels[j], startRow = 4, startCol = j + 1, colNames = FALSE)
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

  setColWidths(wb, sheet, cols = 1, widths = 36)
  setColWidths(wb, sheet, cols = 2:(n_models + 1), widths = 18)

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
  setColWidths(wb, "Notes", cols = 1, widths = 140)

  addWorksheet(wb, "Sample Sizes", gridLines = FALSE)
  writeData(
    wb,
    "Sample Sizes",
    data.frame(
      Model = display_labels,
      N = sapply(models, nobs),
      stringsAsFactors = FALSE
    )
  )
  setColWidths(wb, "Sample Sizes", cols = 1:2, widths = c(30, 12))

  saveWorkbook(wb, xlsx_path, overwrite = TRUE)
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

# convert returns to decimal if needed
if (is.finite(mean(abs(dt0$mret), na.rm = TRUE)) && mean(abs(dt0$mret), na.rm = TRUE) > 1) {
  dt0[, mret := mret / 100]
}

# -----------------------
# 1) Class flags
# -----------------------
dt0[, retail := as.integer(retail_fund == "Y" & (is.na(inst_fund) | inst_fund != "Y"))]
dt0[, inst   := as.integer(inst_fund   == "Y")]

# -----------------------
# 2) Build local contemporaneous controls + Darendeli flow
#    These are LOCAL to this script only
# -----------------------
setorder(dt0, crsp_fundno, caldt)

# Current-period controls
dt0[, age_years_t := as.numeric(caldt - first_offer_dt) / 365.25]
dt0[, log_tna_t   := log1p(mtna)]

# Family TNA at t
dt0[, family_tna_t := sum(mtna, na.rm = TRUE), by = .(mgmt_cd, caldt)]
dt0[, log_familytna_t := fifelse(
  is.na(family_tna_t) | family_tna_t <= 0,
  NA_real_,
  log(family_tna_t)
)]

# Use contemporaneous fee/turnover variables if present
if ("turn_ratio" %in% names(dt0)) dt0[, turn_ratio_t := as.numeric(turn_ratio)] else dt0[, turn_ratio_t := NA_real_]
if ("mgmt_fee"   %in% names(dt0)) dt0[, mgmt_fee_t   := as.numeric(mgmt_fee)]   else dt0[, mgmt_fee_t   := NA_real_]
if ("exp_ratio"  %in% names(dt0)) dt0[, exp_ratio_t  := as.numeric(exp_ratio)]  else dt0[, exp_ratio_t  := NA_real_]

# Darendeli flow at t
dt0[, mtna_l1_local := shift(mtna, 1L), by = crsp_fundno]
dt0[, flow_dar_t := fifelse(
  is.na(mtna_l1_local) | mtna_l1_local <= 0 | is.na(mret) | (1 + mret) <= 0,
  NA_real_,
  (mtna - mtna_l1_local * (1 + mret)) / (mtna_l1_local * (1 + mret))
)]

# Inflow at t = positive part of Darendeli flow
dt0[, inflow_dar_t := fifelse(
  is.na(flow_dar_t),
  NA_real_,
  pmax(flow_dar_t, 0)
)]

# Inflow at t+1 = dependent variable
dt0[, inflow_tp1 := shift(inflow_dar_t, 1L, type = "lead"), by = crsp_fundno]

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
add_sample_specific_fee_tercile <- function(DT) {
  out <- copy(DT)

  fee_base <- out[caldt < as.Date("2024-07-30")]

  fee_class <- fee_base[, .(
    avg_exp_ratio_t = if (all(is.na(exp_ratio_t))) NA_real_ else mean(exp_ratio_t, na.rm = TRUE)
  ), by = crsp_fundno]

  fee_class_nonmiss <- fee_class[!is.na(avg_exp_ratio_t)]

  tcuts <- quantile(
    fee_class_nonmiss$avg_exp_ratio_t,
    probs = c(1/3, 2/3),
    na.rm = TRUE,
    type = 7
  )

  fee_class[, fee_tercile := fifelse(
    is.na(avg_exp_ratio_t), NA_integer_,
    fifelse(avg_exp_ratio_t <= tcuts[1], 1L,
            fifelse(avg_exp_ratio_t <= tcuts[2], 2L, 3L))
  )]

  out <- fee_class[out, on = "crsp_fundno"]
  out
}
# -----------------------
# 4) Column 1 sample: Retail
# -----------------------
dtR <- dt0[retail == 1 & inst == 0]
dtR <- add_post_from_adoption(dtR)

controls_m1_needed <- c(
  "age_years_t",
  "log_tna_t",
  "flow_dar_t",
  "log_familytna_t",
  "turn_ratio_t"
)

controls_m1_R <- controls_m1_needed[controls_m1_needed %in% names(dtR)]
rhs_controls_m1_R <- if (length(controls_m1_R) > 0) paste(controls_m1_R, collapse = " + ") else "0"

# -----------------------
# 5) Column 2 sample: Institutional
# -----------------------
dtI <- dt0[inst == 1]
dtI <- add_post_from_adoption(dtI)

controls_m1_I <- controls_m1_needed[controls_m1_needed %in% names(dtI)]
rhs_controls_m1_I <- if (length(controls_m1_I) > 0) paste(controls_m1_I, collapse = " + ") else "0"

# -----------------------
# 6) Column 3 sample: pooled clean classes + Retail control
# -----------------------
dtC <- dt0[(retail == 1 & inst == 0) | (inst == 1 & retail == 0)]
dtC[, retail := as.integer(retail == 1)]
dtC <- add_post_from_adoption(dtC)

controls_m1_C <- controls_m1_needed[controls_m1_needed %in% names(dtC)]
rhs_controls_m1_C <- if (length(controls_m1_C) > 0) paste(controls_m1_C, collapse = " + ") else "0"

# -----------------------
# 7) Column 4 sample: exact current Model 2 mixed-class logic
#    BUT estimation now uses CLASS FE (crsp_fundno + ym)
# -----------------------
dtD <- dt0[(retail == 1 & inst == 0) | (inst == 1 & retail == 0)]
dtD[, retail := as.integer(retail == 1)]

port_mix <- unique(
  dtD[, .(crsp_portno, crsp_fundno, retail)]
)[
  ,
  .(
    n_retail_classes = uniqueN(crsp_fundno[retail == 1]),
    n_inst_classes   = uniqueN(crsp_fundno[retail == 0])
  ),
  by = crsp_portno
]

mixed_portnos <- port_mix[n_retail_classes > 0 & n_inst_classes > 0, crsp_portno]
dtD <- dtD[crsp_portno %in% mixed_portnos]
dtD <- add_post_from_adoption(dtD)

controls_m2_needed <- c(
  "log_tna_t",
  "age_years_t",
  "flow_dar_t",
  "log_familytna_t",
  "turn_ratio_t"
)
controls_m2_D <- controls_m2_needed[controls_m2_needed %in% names(dtD)]
rhs_controls_m2_D <- if (length(controls_m2_D) > 0) paste(controls_m2_D, collapse = " + ") else "0"

# -----------------------
# 7B) Drop missing FIRST, then winsorize, then assign terciles
# -----------------------

winsor_vars <- c(
  "inflow_tp1",
  "inflow_dar_t",
  "perf_t",
  "age_years_t",
  "log_tna_t",
  "flow_dar_t",
  "log_familytna_t",
  "turn_ratio_t",
  "mgmt_fee_t",
  "exp_ratio_t"
)

# ---- Column 1: Retail ----
if (length(controls_m1_R) > 0) {
  for (cc in controls_m1_R) {
    suppressWarnings(dtR[, (cc) := as.numeric(get(cc))])
    dtR[is.na(get(cc)), (cc) := 0]
  }
}
core_keep_R <- c("inflow_tp1","perf_t","post_fund","crsp_fundno","crsp_portno","ym")
dtR <- dtR[complete.cases(dtR[, ..core_keep_R])]
dtR_win <- winsorize_dt(dtR, winsor_vars, probs = c(0.01, 0.99))

dtR     <- add_sample_specific_fee_tercile(dtR)
dtR_win <- add_sample_specific_fee_tercile(dtR_win)

# ---- Column 2: Institutional ----
if (length(controls_m1_I) > 0) {
  for (cc in controls_m1_I) {
    suppressWarnings(dtI[, (cc) := as.numeric(get(cc))])
    dtI[is.na(get(cc)), (cc) := 0]
  }
}
core_keep_I <- c("inflow_tp1","perf_t","post_fund","crsp_fundno","crsp_portno","ym")
dtI <- dtI[complete.cases(dtI[, ..core_keep_I])]
dtI_win <- winsorize_dt(dtI, winsor_vars, probs = c(0.01, 0.99))

dtI     <- add_sample_specific_fee_tercile(dtI)
dtI_win <- add_sample_specific_fee_tercile(dtI_win)

# ---- Column 3: Combined classes ----
if (length(controls_m1_C) > 0) {
  for (cc in controls_m1_C) {
    suppressWarnings(dtC[, (cc) := as.numeric(get(cc))])
    dtC[is.na(get(cc)), (cc) := 0]
  }
}
core_keep_C <- c("inflow_tp1","perf_t","post_fund","retail","crsp_fundno","crsp_portno","ym")
dtC <- dtC[complete.cases(dtC[, ..core_keep_C])]
dtC_win <- winsorize_dt(dtC, winsor_vars, probs = c(0.01, 0.99))

dtC     <- add_sample_specific_fee_tercile(dtC)
dtC_win <- add_sample_specific_fee_tercile(dtC_win)

# ---- Column 4: DiD ----
if (length(controls_m2_D) > 0) {
  for (cc in controls_m2_D) {
    suppressWarnings(dtD[, (cc) := as.numeric(get(cc))])
  }
}
keep_D <- c("inflow_tp1","perf_t","post_fund","retail","crsp_fundno","crsp_portno","ym", controls_m2_D)
dtD <- dtD[complete.cases(dtD[, ..keep_D])]
dtD_win <- winsorize_dt(dtD, winsor_vars, probs = c(0.01, 0.99))

dtD     <- add_sample_specific_fee_tercile(dtD)
dtD_win <- add_sample_specific_fee_tercile(dtD_win)

run_professor_table <- function(dtR_in, dtI_in, dtC_in, dtD_in, out_subdir, fee_group = c("t1", "t2", "t3")) {
  fee_group <- match.arg(fee_group)
  dtR <- copy(dtR_in)
  dtI <- copy(dtI_in)
  dtC <- copy(dtC_in)
  dtD <- copy(dtD_in)

  tercile_num <- switch(
    fee_group,
    t1 = 1L,
    t2 = 2L,
    t3 = 3L
  )

  dtR <- dtR[fee_tercile == tercile_num]
  dtI <- dtI[fee_tercile == tercile_num]
  dtC <- dtC[fee_tercile == tercile_num]
  dtD <- dtD[fee_tercile == tercile_num]

  OUT_DIR_RUN <- file.path(
    get("proj_root", envir = .GlobalEnv),
    "Results",
    "professor_alpha_capm_12m_dar_inflow_tplus1_classFE",
    paste0(fee_group, "_fee_tercile"),
    out_subdir
  )
  dir.create(OUT_DIR_RUN, recursive = TRUE, showWarnings = FALSE)

  # -----------------------
  # 9) Estimate models
  # -----------------------

  # Column 1: Model 1 - Retail
  fml_R <- as.formula(paste0(
    "inflow_tp1 ~ perf_t * post_fund + ",
    if (rhs_controls_m1_R != "0") rhs_controls_m1_R else "0",
    " | crsp_fundno + ym"
  ))
  m_retail <- feols(fml_R, data = dtR, cluster = ~ crsp_portno)

  # Column 2: Model 1 - Institutional
  fml_I <- as.formula(paste0(
    "inflow_tp1 ~ perf_t * post_fund + ",
    if (rhs_controls_m1_I != "0") rhs_controls_m1_I else "0",
    " | crsp_fundno + ym"
  ))
  m_inst <- feols(fml_I, data = dtI, cluster = ~ crsp_portno)

  # Column 3: Combined classes
  fml_C <- as.formula(paste0(
    "inflow_tp1 ~ perf_t * post_fund + retail + ",
    if (rhs_controls_m1_C != "0") rhs_controls_m1_C else "0",
    " | crsp_fundno + ym"
  ))
  m_combined <- feols(fml_C, data = dtC, cluster = ~ crsp_portno)

  # Column 4: DiD - Model 2 with CLASS FE
  fml_D <- as.formula(paste0(
    "inflow_tp1 ~ perf_t * post_fund * retail + ",
    if (rhs_controls_m2_D != "0") rhs_controls_m2_D else "0",
    " | crsp_fundno + ym"
  ))
  m_did <- feols(fml_D, data = dtD, cluster = ~ crsp_portno)

  # -----------------------
  # 10) Output maps and labels
  # -----------------------
  models <- list(
    "Model 1 - Retail"        = m_retail,
    "Model 1 - Institutional" = m_inst,
    "Combined classes"        = m_combined,
    "DiD - Model 2"           = m_did
  )

  coef_map <- c(
    "perf_t"                   = "Perf (t)",
    "post_fund"                = "Post (fund TSR adoption)",
    "perf_t:post_fund"         = "Perf (t) × Post",
    "retail"                   = "Retail indicator",
    "perf_t:retail"            = "Perf (t) × Retail",
    "post_fund:retail"         = "Post × Retail",
    "perf_t:post_fund:retail"  = "Perf (t) × Post × Retail",
    "age_years_t"              = "Fund age (t)",
    "log_tna_t"                = "Log(TNA) (t)",
    "flow_dar_t"               = "Darendeli flow (t)",
    "log_familytna_t"          = "Log(Family TNA) (t)",
    "turn_ratio_t"             = "Turnover (t)"
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
    "Model 1 - Retail" = c(
      "Yes",
      "Yes",
      "crsp_portno",
      "Darendeli inflow (t+1)",
      sprintf("%.3f", fitstat(m_retail, "r2")),
      sprintf("%.3f", fitstat(m_retail, "ar2")),
      sprintf("%.3f", fitstat(m_retail, "wr2")),
      sprintf("%.3f", fitstat(m_retail, "war2"))
    ),
    "Model 1 - Institutional" = c(
      "Yes",
      "Yes",
      "crsp_portno",
      "Darendeli inflow (t+1)",
      sprintf("%.3f", fitstat(m_inst, "r2")),
      sprintf("%.3f", fitstat(m_inst, "ar2")),
      sprintf("%.3f", fitstat(m_inst, "wr2")),
      sprintf("%.3f", fitstat(m_inst, "war2"))
    ),
    "Combined classes" = c(
      "Yes",
      "Yes",
      "crsp_portno",
      "Darendeli inflow (t+1)",
      sprintf("%.3f", fitstat(m_combined, "r2")),
      sprintf("%.3f", fitstat(m_combined, "ar2")),
      sprintf("%.3f", fitstat(m_combined, "wr2")),
      sprintf("%.3f", fitstat(m_combined, "war2"))
    ),
    "DiD - Model 2" = c(
      "Yes",
      "Yes",
      "crsp_portno",
      "Darendeli inflow (t+1)",
      sprintf("%.3f", fitstat(m_did, "r2")),
      sprintf("%.3f", fitstat(m_did, "ar2")),
      sprintf("%.3f", fitstat(m_did, "wr2")),
      sprintf("%.3f", fitstat(m_did, "war2"))
    ),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  notes_txt <- paste0(
    "Fee subsample: ", toupper(fee_group), " tercile of pre-TSR average expense ratio. ",
    "Dependent variable: Darendeli flow at t+1. ",
    "Darendeli flow is defined locally in this script as (TNA_t - TNA_{t-1}(1+R_t)) / (TNA_{t-1}(1+R_t)), ",
    "and the regression outcome uses its one-month lead. ",
    "Performance variable: alpha_capm_12m at t. ",
    "Controls are measured at t. ",
    "All models use class fixed effects (crsp_fundno) plus year-month fixed effects. ",
    "t statistics in parentheses. Standard errors are clustered by crsp_portno. ",
    "This script does not modify dt_24 or any saved pipeline data."
  )

  export_formatted_table(
    models = models,
    coef_map = coef_map,
    gof_map = gof_map,
    add_rows = add_rows,
    notes_txt = notes_txt,
    title_text = paste0("Table 5  Determinants analysis: ", toupper(fee_group), " fee tercile"),
    display_labels = c("Model 1 - Retail", "Model 1 - Institutional", "Combined classes", "DiD - Model 2"),
    html_path = file.path(OUT_DIR_RUN, "professor_table_alpha_capm_12m_dar_tplus1_classFE.html"),
    tex_path  = file.path(OUT_DIR_RUN, "professor_table_alpha_capm_12m_dar_tplus1_classFE.tex"),
    xlsx_path = file.path(OUT_DIR_RUN, "professor_table_alpha_capm_12m_dar_tplus1_classFE.xlsx")
  )

  cat("\n===================== PROFESSOR TABLE BUILT =====================\n")
  cat("Output folder:\n", OUT_DIR_RUN, "\n\n")
  cat("Files created:\n")
  cat(" - professor_table_alpha_capm_12m_dar_tplus1_classFE.html\n")
  cat(" - professor_table_alpha_capm_12m_dar_tplus1_classFE.tex\n")
  cat(" - professor_table_alpha_capm_12m_dar_tplus1_classFE.xlsx\n\n")

  cat("Sample sizes:\n")
  cat("Model 1 - Retail:        ", nobs(m_retail), "\n")
  cat("Model 1 - Institutional: ", nobs(m_inst), "\n")
  cat("Combined classes:        ", nobs(m_combined), "\n")
  cat("DiD - Model 2:           ", nobs(m_did), "\n")

  return(list(
    m_retail   = m_retail,
    m_inst     = m_inst,
    m_combined = m_combined,
    m_did      = m_did
  ))
}

# -----------------------
# 15) Build ONE combined quartile table for each model
# -----------------------

tercile_results_nowin <- list()
tercile_results_win   <- list()

for (fg in c("t1", "t2", "t3")) {
  tercile_results_nowin[[fg]] <- run_professor_table(
    dtR_in = dtR,
    dtI_in = dtI,
    dtC_in = dtC,
    dtD_in = dtD,
    out_subdir = "without_winsorization",
    fee_group = fg
  )

  tercile_results_win[[fg]] <- run_professor_table(
    dtR_in = dtR_win,
    dtI_in = dtI_win,
    dtC_in = dtC_win,
    dtD_in = dtD_win,
    out_subdir = "with_winsorization",
    fee_group = fg
  )
}
build_tercile_summary_tables <- function(results_list, out_subdir) {

  OUT_DIR_SUM <- file.path(
    get("proj_root", envir = .GlobalEnv),
    "Results",
    "professor_alpha_capm_12m_dar_inflow_tplus1_classFE",
    out_subdir,
    "tercile_summary_tables"
  )
  dir.create(OUT_DIR_SUM, recursive = TRUE, showWarnings = FALSE)

  coef_map <- c(
    "perf_t"                   = "Perf (t)",
    "post_fund"                = "Post (fund TSR adoption)",
    "perf_t:post_fund"         = "Perf (t) × Post",
    "retail"                   = "Retail indicator",
    "perf_t:retail"            = "Perf (t) × Retail",
    "post_fund:retail"         = "Post × Retail",
    "perf_t:post_fund:retail"  = "Perf (t) × Post × Retail",
    "age_years_t"              = "Fund age (t)",
    "log_tna_t"                = "Log(TNA) (t)",
    "flow_dar_t"               = "Darendeli flow (t)",
    "log_familytna_t"          = "Log(Family TNA) (t)",
    "turn_ratio_t"             = "Turnover (t)"
  )

  gof_map <- data.frame(
    raw = c("nobs"),
    clean = c("N"),
    fmt = c(0),
    stringsAsFactors = FALSE
  )

  model_sets <- list(
    retail = list(
      "T1" = results_list$t1$m_retail,
      "T2" = results_list$t2$m_retail,
      "T3" = results_list$t3$m_retail
    ),
    inst = list(
      "T1" = results_list$t1$m_inst,
      "T2" = results_list$t2$m_inst,
      "T3" = results_list$t3$m_inst
    ),
    combined = list(
      "T1" = results_list$t1$m_combined,
      "T2" = results_list$t2$m_combined,
      "T3" = results_list$t3$m_combined
    ),
    did = list(
      "T1" = results_list$t1$m_did,
      "T2" = results_list$t2$m_did,
      "T3" = results_list$t3$m_did
    )
  )

  titles <- c(
    retail   = "tercile_summary_model1_retail",
    inst     = "tercile_summary_model1_institutional",
    combined = "tercile_summary_combined_classes",
    did      = "tercile_summary_model2_did"
  )

  pretty_titles <- c(
    retail   = "Table 5  Tercile summary: Model 1 - Retail",
    inst     = "Table 5  Tercile summary: Model 1 - Institutional",
    combined = "Table 5  Tercile summary: Combined classes",
    did      = "Table 5  Tercile summary: DiD - Model 2"
  )

  for (nm in names(model_sets)) {
    ms <- model_sets[[nm]]

    if (nm == "did") {
      zt <- compute_z_from_models(ms[["T3"]], ms[["T1"]], parts = c("perf_t", "post_fund", "retail"))
      diff_label <- "Coeff. difference on Perf (t) × Post × Retail [Q3 - Q1]"
      p_label    <- "P-value for coeff. difference [Q3 - Q1 pair]"
    } else {
      zt <- compute_z_from_models(ms[["T3"]], ms[["T1"]], parts = c("perf_t", "post_fund"))
      diff_label <- "Coeff. difference on Perf (t) × Post [Q3 - Q1]"
      p_label    <- "P-value for coeff. difference [Q3 - Q1 pair]"
    }
    add_rows_sum <- rbind(
      make_test_row(
        diff_label,
        idx = 1,
        value = fmt_num(zt$diff),
        cn = names(ms)
      ),
      make_test_row(
        p_label,
        idx = 1,
        value = fmt_num(zt$p),
        cn = names(ms)
      )
    )

    notes_txt <- paste0(
      "Columns are fee terciles based on pre-TSR average expense ratio. ",
      "Dependent variable: Darendeli flow at t+1. ",
      "Darendeli flow is defined as (TNA_t - TNA_{t-1}(1+R_t)) / (TNA_{t-1}(1+R_t)). ",
      "Performance variable: alpha_capm_12m at t. ",
      "Controls are measured at t. ",
      "All models use class fixed effects (crsp_fundno) plus year-month fixed effects. ",
      "Q1 vs Q3 coefficient comparison reports the p-value from the professor-requested test based on ",
      "z = (Beta_Q3 - Beta_Q1) / sqrt(SE_Q3^2 + SE_Q1^2). The Q1-Q3 comparison is shown once for the pair. ",
      "t statistics in parentheses. Standard errors are clustered by crsp_portno."
    )

    export_formatted_table(
      models = ms,
      coef_map = coef_map,
      gof_map = gof_map,
      add_rows = add_rows_sum,
      notes_txt = notes_txt,
      title_text = pretty_titles[[nm]],
      display_labels = c("T1", "T2", "T3"),
      html_path = file.path(OUT_DIR_SUM, paste0(titles[[nm]], ".html")),
      tex_path  = file.path(OUT_DIR_SUM, paste0(titles[[nm]], ".tex")),
      xlsx_path = file.path(OUT_DIR_SUM, paste0(titles[[nm]], ".xlsx"))
    )
  }
}

build_tercile_summary_tables(
  results_list = tercile_results_nowin,
  out_subdir = "without_winsorization"
)

build_tercile_summary_tables(
  results_list = tercile_results_win,
  out_subdir = "with_winsorization"
)