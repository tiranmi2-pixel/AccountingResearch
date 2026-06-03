# ===================== PROFESSOR TABLE: alpha_capm_12m (OUTFLOW t+1, t REGRESSORS, HIGH-vs-LOW FEE COMBINED TABLE) =====================
# PURPOSE:
#   Build ONE 6-column professor table:
#     (1) High-fee Combined
#     (2) Low-fee Combined
#     (3) High-fee Retail
#     (4) Low-fee Retail
#     (5) High-fee Institutional
#     (6) Low-fee Institutional
#
# IMPORTANT:
#   - Core logic of the three displayed model families is unchanged
#   - Model 2 / DiD removed per instruction
#   - Dependent variable = outflow at t+1
#   - Performance variable = alpha_capm_12m at t
#   - Controls are measured at t
#   - Fee split unchanged: permanent high-fee / low-fee bucket based on pre-2024-07-30 average expense ratio
#   - Winsorization timing unchanged: full dataset winsorized BEFORE splitting
#   - All displayed models use CLASS fixed effects: crsp_fundno + ym
#   - Comparison rows now use professor-requested z-test:
#       z = (Beta_high - Beta_low) / sqrt(SE_high^2 + SE_low^2)
#   - Output formatting upgraded; filenames now reflect content

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(lubridate)
  library(modelsummary)
  library(openxlsx)
  library(kableExtra)
})

# -----------------------
# Helpers
# -----------------------
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

blank_like <- function(x) {
  y <- x[0, , drop = FALSE]
  y[1, ] <- ""
  y
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

prepare_model_sample <- function(DT, controls, core_keep) {
  out <- copy(DT)
  
  if (length(controls) > 0) {
    for (cc in controls) {
      suppressWarnings(out[, (cc) := as.numeric(get(cc))])
      out[is.na(get(cc)), (cc) := 0]
    }
  }
  
  keep_vars <- unique(c(core_keep, "high_fee"))
  out <- out[!is.na(high_fee)]
  out <- out[complete.cases(out[, ..keep_vars])]
  out
}

# professor-requested comparison helpers
extract_term_est_se <- function(model, parts = c("perf_t", "post_fund")) {
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

compute_z_from_models <- function(high_model, low_model, parts = c("perf_t", "post_fund")) {
  hi <- extract_term_est_se(high_model, parts = parts)
  lo <- extract_term_est_se(low_model,  parts = parts)
  
  diff <- hi$beta - lo$beta
  zval <- diff / sqrt(hi$se^2 + lo$se^2)
  
  list(
    beta_high = hi$beta,
    se_high   = hi$se,
    beta_low  = lo$beta,
    se_low    = lo$se,
    diff      = diff,
    z         = zval
  )
}

compute_z_tests <- function(res_high, res_low) {
  list(
    combined = compute_z_from_models(res_high$models$combined, res_low$models$combined),
    retail   = compute_z_from_models(res_high$models$retail,   res_low$models$retail),
    institutional = compute_z_from_models(res_high$models$inst, res_low$models$inst)
  )
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
# 2) Local contemporaneous controls + Darendeli flow
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

if ("turn_ratio" %in% names(dt0)) dt0[, turn_ratio_t := as.numeric(turn_ratio)] else dt0[, turn_ratio_t := NA_real_]
if ("mgmt_fee"   %in% names(dt0)) dt0[, mgmt_fee_t   := as.numeric(mgmt_fee)]   else dt0[, mgmt_fee_t   := NA_real_]
if ("exp_ratio"  %in% names(dt0)) dt0[, exp_ratio_t  := as.numeric(exp_ratio)]  else dt0[, exp_ratio_t  := NA_real_]

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
# 2B) Permanent high-fee / low-fee split
# -----------------------
fee_base <- dt0[caldt < as.Date("2024-07-30")]

fee_class <- fee_base[, .(
  avg_exp_ratio_t = if (all(is.na(exp_ratio_t))) NA_real_ else mean(exp_ratio_t, na.rm = TRUE)
), by = crsp_fundno]

fee_cutoff <- median(fee_class$avg_exp_ratio_t, na.rm = TRUE)

fee_class[, high_fee := as.integer(avg_exp_ratio_t > fee_cutoff)]
fee_class[, low_fee  := as.integer(avg_exp_ratio_t <= fee_cutoff)]
fee_class[is.na(avg_exp_ratio_t), `:=`(high_fee = NA_integer_, low_fee = NA_integer_)]

dt0 <- fee_class[dt0, on = "crsp_fundno"]

# -----------------------
# 3) Fund-specific post at portno level
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
  "turn_ratio_t"
)

# -----------------------
# 4) Samples for displayed models
# -----------------------
dtR <- dt0[retail == 1 & inst == 0]
dtR <- add_post_from_adoption(dtR)
controls_m1_R <- controls_m1_needed[controls_m1_needed %in% names(dtR)]
rhs_controls_m1_R <- if (length(controls_m1_R) > 0) paste(controls_m1_R, collapse = " + ") else "0"

dtI <- dt0[inst == 1]
dtI <- add_post_from_adoption(dtI)
controls_m1_I <- controls_m1_needed[controls_m1_needed %in% names(dtI)]
rhs_controls_m1_I <- if (length(controls_m1_I) > 0) paste(controls_m1_I, collapse = " + ") else "0"

dtC <- dt0[(retail == 1 & inst == 0) | (inst == 1 & retail == 0)]
dtC[, retail := as.integer(retail == 1)]
dtC <- add_post_from_adoption(dtC)
controls_m1_C <- controls_m1_needed[controls_m1_needed %in% names(dtC)]
rhs_controls_m1_C <- if (length(controls_m1_C) > 0) paste(controls_m1_C, collapse = " + ") else "0"

# -----------------------
# 5) Winsorize full dataset BEFORE splitting
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

# ============================================================
# FIT DISPLAYED MODELS FOR ONE FEE GROUP
# ============================================================
fit_fee_models <- function(dtR_in, dtI_in, dtC_in, fee_group = c("high", "low")) {
  fee_group <- match.arg(fee_group)
  
  dtR <- copy(dtR_in)
  dtI <- copy(dtI_in)
  dtC <- copy(dtC_in)
  
  if (fee_group == "high") {
    dtR <- dtR[high_fee == 1]
    dtI <- dtI[high_fee == 1]
    dtC <- dtC[high_fee == 1]
  } else {
    dtR <- dtR[low_fee == 1]
    dtI <- dtI[low_fee == 1]
    dtC <- dtC[low_fee == 1]
  }
  
  dtR <- prepare_model_sample(
    dtR,
    controls = controls_m1_R,
    core_keep = c("outflow_tp1","perf_t","post_fund","crsp_fundno","crsp_portno","ym")
  )
  
  dtI <- prepare_model_sample(
    dtI,
    controls = controls_m1_I,
    core_keep = c("outflow_tp1","perf_t","post_fund","crsp_fundno","crsp_portno","ym")
  )
  
  dtC <- prepare_model_sample(
    dtC,
    controls = controls_m1_C,
    core_keep = c("outflow_tp1","perf_t","post_fund","retail","crsp_fundno","crsp_portno","ym")
  )
  
  fml_C <- as.formula(paste0(
    "outflow_tp1 ~ perf_t * post_fund + retail + ",
    if (rhs_controls_m1_C != "0") rhs_controls_m1_C else "0",
    " | crsp_fundno + ym"
  ))
  m_combined <- feols(fml_C, data = dtC, cluster = ~ crsp_portno)
  
  fml_R <- as.formula(paste0(
    "outflow_tp1 ~ perf_t * post_fund + ",
    if (rhs_controls_m1_R != "0") rhs_controls_m1_R else "0",
    " | crsp_fundno + ym"
  ))
  m_retail <- feols(fml_R, data = dtR, cluster = ~ crsp_portno)
  
  fml_I <- as.formula(paste0(
    "outflow_tp1 ~ perf_t * post_fund + ",
    if (rhs_controls_m1_I != "0") rhs_controls_m1_I else "0",
    " | crsp_fundno + ym"
  ))
  m_inst <- feols(fml_I, data = dtI, cluster = ~ crsp_portno)
  
  list(
    fee_group = fee_group,
    models = list(
      combined = m_combined,
      retail   = m_retail,
      inst     = m_inst
    )
  )
}

# ============================================================
# EXPORT ONE 6-COLUMN TABLE
# ============================================================
export_combined_fee_table <- function(res_high, res_low, z_tests, out_subdir) {
  OUT_DIR_RUN <- file.path(
    get("proj_root", envir = .GlobalEnv),
    "Results",
    "alpha_capm12m_outflow_tplus1_classFE_highlow_comparison",
    out_subdir
  )
  dir.create(OUT_DIR_RUN, recursive = TRUE, showWarnings = FALSE)
  
  FILE_STEM <- "alpha_capm12m_outflow_tplus1_classFE_highlow_comparison"
  
  model_cols <- c(
    "High fee - Combined",
    "Low fee - Combined",
    "High fee - Retail",
    "Low fee - Retail",
    "High fee - Institutional",
    "Low fee - Institutional"
  )
  
  models <- list(
    "High fee - Combined"      = res_high$models$combined,
    "Low fee - Combined"       = res_low$models$combined,
    "High fee - Retail"        = res_high$models$retail,
    "Low fee - Retail"         = res_low$models$retail,
    "High fee - Institutional" = res_high$models$inst,
    "Low fee - Institutional"  = res_low$models$inst
  )
  
  display_labels <- c(
    "High fee", "Low fee",
    "High fee", "Low fee",
    "High fee", "Low fee"
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
    "turn_ratio_t"     = "Turnover (t)"
  )
  
  gof_map <- data.frame(
    raw = c("nobs"),
    clean = c("N"),
    fmt = c(0),
    stringsAsFactors = FALSE
  )
  
  add_rows_main <- data.frame(
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
    "High fee - Combined" = c(
      "Yes","Yes","crsp_portno","Outflow (t+1)",
      fmt_num(fitstat(res_high$models$combined, "r2")),
      fmt_num(fitstat(res_high$models$combined, "ar2")),
      fmt_num(fitstat(res_high$models$combined, "wr2")),
      fmt_num(fitstat(res_high$models$combined, "war2"))
    ),
    "Low fee - Combined" = c(
      "Yes","Yes","crsp_portno","Outflow (t+1)",
      fmt_num(fitstat(res_low$models$combined, "r2")),
      fmt_num(fitstat(res_low$models$combined, "ar2")),
      fmt_num(fitstat(res_low$models$combined, "wr2")),
      fmt_num(fitstat(res_low$models$combined, "war2"))
    ),
    "High fee - Retail" = c(
      "Yes","Yes","crsp_portno","Outflow (t+1)",
      fmt_num(fitstat(res_high$models$retail, "r2")),
      fmt_num(fitstat(res_high$models$retail, "ar2")),
      fmt_num(fitstat(res_high$models$retail, "wr2")),
      fmt_num(fitstat(res_high$models$retail, "war2"))
    ),
    "Low fee - Retail" = c(
      "Yes","Yes","crsp_portno","Outflow (t+1)",
      fmt_num(fitstat(res_low$models$retail, "r2")),
      fmt_num(fitstat(res_low$models$retail, "ar2")),
      fmt_num(fitstat(res_low$models$retail, "wr2")),
      fmt_num(fitstat(res_low$models$retail, "war2"))
    ),
    "High fee - Institutional" = c(
      "Yes","Yes","crsp_portno","Outflow (t+1)",
      fmt_num(fitstat(res_high$models$inst, "r2")),
      fmt_num(fitstat(res_high$models$inst, "ar2")),
      fmt_num(fitstat(res_high$models$inst, "wr2")),
      fmt_num(fitstat(res_high$models$inst, "war2"))
    ),
    "Low fee - Institutional" = c(
      "Yes","Yes","crsp_portno","Outflow (t+1)",
      fmt_num(fitstat(res_low$models$inst, "r2")),
      fmt_num(fitstat(res_low$models$inst, "ar2")),
      fmt_num(fitstat(res_low$models$inst, "wr2")),
      fmt_num(fitstat(res_low$models$inst, "war2"))
    ),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  
  add_rows_tests <- rbind(
    make_test_row(
      "Coeff. difference on Perf (t) × Post [Combined]",
      idx = c(1, 2),
      value = fmt_num(z_tests$combined$diff),
      cn = model_cols
    ),
    make_test_row(
      "Z-score for coeff. difference [Combined]",
      idx = c(1, 2),
      value = fmt_num(z_tests$combined$z),
      cn = model_cols
    ),
    make_test_row(
      "Coeff. difference on Perf (t) × Post [Retail]",
      idx = c(3, 4),
      value = fmt_num(z_tests$retail$diff),
      cn = model_cols
    ),
    make_test_row(
      "Z-score for coeff. difference [Retail]",
      idx = c(3, 4),
      value = fmt_num(z_tests$retail$z),
      cn = model_cols
    ),
    make_test_row(
      "Coeff. difference on Perf (t) × Post [Institutional]",
      idx = c(5, 6),
      value = fmt_num(z_tests$institutional$diff),
      cn = model_cols
    ),
    make_test_row(
      "Z-score for coeff. difference [Institutional]",
      idx = c(5, 6),
      value = fmt_num(z_tests$institutional$z),
      cn = model_cols
    )
  )
  
  add_rows <- rbind(add_rows_main, add_rows_tests)
  
  table_title <- "Table 5  Determinants analysis: High-fee vs Low-fee"
  
  notes_txt <- paste0(
    "Columns (1)-(2) compare high-fee vs low-fee Combined classes; ",
    "columns (3)-(4) compare high-fee vs low-fee Retail classes; ",
    "columns (5)-(6) compare high-fee vs low-fee Institutional classes. ",
    "Fee buckets are permanent and based on each class's pre-2024-07-30 average expense ratio. ",
    "Dependent variable: outflow at t+1. ",
    "Darendeli flow is defined as (TNA_t - TNA_{t-1}(1+R_t)) / (TNA_{t-1}(1+R_t)). ",
    "Outflow is defined as the negative part of Darendeli flow, made positive, i.e. max(-Darendeli flow, 0), ",
    "and the dependent variable is its one-month lead. ",
    "Performance variable: alpha_capm_12m at t. Controls are measured at t. ",
    "Coefficient comparisons now use the professor-requested z-test based on separately estimated coefficients: ",
    "z = (Beta_high - Beta_low) / sqrt(SE_high^2 + SE_low^2). ",
    "The table reports the coefficient difference (Beta_high - Beta_low) and the corresponding z-score."
  )
  
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
  
  # HTML
  html_table <- kbl(
    tab_df,
    format = "html",
    escape = FALSE,
    align = c("l", rep("c", 6)),
    col.names = c("", display_labels),
    booktabs = TRUE,
    linesep = ""
  ) |>
    add_header_above(c(" " = 1, "(1)" = 1, "(2)" = 1, "(3)" = 1, "(4)" = 1, "(5)" = 1, "(6)" = 1),
                     bold = FALSE, align = "center") |>
    add_header_above(c(" " = 1, "Combined classes" = 2, "Retail" = 2, "Institutional" = 2),
                     bold = FALSE, align = "center") |>
    kable_styling(
      full_width = FALSE,
      position = "center",
      font_size = 15,
      html_font = "Times New Roman"
    ) |>
    row_spec(0, extra_css = "border-top: 2px solid black; border-bottom: 1px solid black;") |>
    column_spec(1, width = "30em") |>
    column_spec(2:7, width = "11em")
  
  html_title <- paste0(
    "<div style='width:100%; max-width:1450px; margin:0 auto; font-family:\"Times New Roman\", serif;'>",
    "<div style='font-size:20px; margin-bottom:10px;'>",
    "<span style='font-weight:bold;'>", table_title, "</span>",
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
    con = file.path(OUT_DIR_RUN, paste0(FILE_STEM, ".html"))
  )
  
  # TeX
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
    title = table_title,
    output = file.path(OUT_DIR_RUN, paste0(FILE_STEM, ".tex"))
  )
  
  # CSV
  csv_title <- blank_like(tab_df)
  csv_title[1, 1] <- table_title
  
  csv_group <- blank_like(tab_df)
  csv_group[1, 2] <- "Combined classes"
  csv_group[1, 4] <- "Retail"
  csv_group[1, 6] <- "Institutional"
  
  csv_numrow <- blank_like(tab_df)
  csv_numrow[1, 2:7] <- paste0("(", 1:6, ")")
  
  csv_labrow <- blank_like(tab_df)
  csv_labrow[1, 2:7] <- display_labels
  
  csv_out <- rbind(csv_title, csv_group, csv_numrow, csv_labrow, tab_df)
  
  fwrite(
    csv_out,
    file = file.path(OUT_DIR_RUN, paste0(FILE_STEM, ".csv"))
  )
  
  # Excel
  wb <- createWorkbook()
  addWorksheet(wb, "Regression Table", gridLines = FALSE)
  sheet <- "Regression Table"
  
  title_style <- createStyle(
    fontName = "Times New Roman", fontSize = 14,
    textDecoration = "bold", halign = "left",
    border = "bottom", borderStyle = "thick"
  )
  group_style <- createStyle(
    fontName = "Times New Roman", fontSize = 12,
    halign = "center", textDecoration = "bold"
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
  
  writeData(wb, sheet, x = table_title, startRow = 1, startCol = 1, colNames = FALSE)
  mergeCells(wb, sheet, cols = 1:7, rows = 1)
  addStyle(wb, sheet, title_style, rows = 1, cols = 1:7, gridExpand = TRUE)
  
  writeData(wb, sheet, x = "Combined classes", startRow = 3, startCol = 2, colNames = FALSE)
  mergeCells(wb, sheet, cols = 2:3, rows = 3)
  writeData(wb, sheet, x = "Retail", startRow = 3, startCol = 4, colNames = FALSE)
  mergeCells(wb, sheet, cols = 4:5, rows = 3)
  writeData(wb, sheet, x = "Institutional", startRow = 3, startCol = 6, colNames = FALSE)
  mergeCells(wb, sheet, cols = 6:7, rows = 3)
  addStyle(wb, sheet, group_style, rows = 3, cols = 2:7, gridExpand = TRUE)
  
  for (j in 1:6) {
    writeData(wb, sheet, x = paste0("(", j, ")"), startRow = 4, startCol = j + 1, colNames = FALSE)
  }
  addStyle(wb, sheet, num_style, rows = 4, cols = 1:7, gridExpand = TRUE)
  
  for (j in 1:6) {
    writeData(wb, sheet, x = display_labels[j], startRow = 5, startCol = j + 1, colNames = FALSE)
  }
  addStyle(wb, sheet, label_style, rows = 5, cols = 1:7, gridExpand = TRUE)
  
  writeData(wb, sheet, x = tab_df, startRow = 7, startCol = 1, colNames = FALSE)
  n_body <- nrow(tab_df)
  
  addStyle(wb, sheet, body_left_style, rows = 7:(6 + n_body), cols = 1, gridExpand = TRUE)
  addStyle(wb, sheet, body_center_style, rows = 7:(6 + n_body), cols = 2:7, gridExpand = TRUE)
  
  stat_rows <- which(tab_df[[1]] == "") + 6
  if (length(stat_rows) > 0) {
    addStyle(wb, sheet, stat_style, rows = stat_rows, cols = 2:7, gridExpand = TRUE, stack = TRUE)
  }
  
  sum_start <- which(tab_df[[1]] %in% c("N", "Observations"))[1]
  if (!is.na(sum_start)) {
    excel_row <- sum_start + 6
    addStyle(wb, sheet, summary_top_style, rows = excel_row, cols = 1:7, gridExpand = TRUE, stack = TRUE)
  }
  
  setColWidths(wb, sheet, cols = 1, widths = 38)
  setColWidths(wb, sheet, cols = 2:7, widths = 16)
  
  note_row <- 9 + n_body
  writeData(wb, sheet, x = "Notes:", startRow = note_row, startCol = 1, colNames = FALSE)
  writeData(wb, sheet, x = notes_txt, startRow = note_row + 1, startCol = 1, colNames = FALSE)
  mergeCells(wb, sheet, cols = 1:7, rows = note_row + 1)
  addStyle(wb, sheet, notes_style, rows = note_row:(note_row + 1), cols = 1:7, gridExpand = TRUE)
  
  addWorksheet(wb, "Equality_Tests", gridLines = FALSE)
  eq_tests <- data.frame(
    Pair = c(
      "Col (1) vs (2): High vs Low Combined",
      "Col (3) vs (4): High vs Low Retail",
      "Col (5) vs (6): High vs Low Institutional"
    ),
    Tested_coefficient = "Perf (t) × Post",
    Beta_High = c(
      z_tests$combined$beta_high,
      z_tests$retail$beta_high,
      z_tests$institutional$beta_high
    ),
    SE_High = c(
      z_tests$combined$se_high,
      z_tests$retail$se_high,
      z_tests$institutional$se_high
    ),
    Beta_Low = c(
      z_tests$combined$beta_low,
      z_tests$retail$beta_low,
      z_tests$institutional$beta_low
    ),
    SE_Low = c(
      z_tests$combined$se_low,
      z_tests$retail$se_low,
      z_tests$institutional$se_low
    ),
    Coeff_Difference_High_minus_Low = c(
      z_tests$combined$diff,
      z_tests$retail$diff,
      z_tests$institutional$diff
    ),
    Z_score = c(
      z_tests$combined$z,
      z_tests$retail$z,
      z_tests$institutional$z
    ),
    Formula = "(Beta_high - Beta_low) / sqrt(SE_high^2 + SE_low^2)",
    stringsAsFactors = FALSE
  )
  writeData(wb, "Equality_Tests", eq_tests)
  setColWidths(wb, "Equality_Tests", cols = 1:ncol(eq_tests), widths = c(34, 20, 12, 10, 12, 10, 18, 12, 42))
  
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
      Model = model_cols,
      N = c(
        nobs(res_high$models$combined), nobs(res_low$models$combined),
        nobs(res_high$models$retail),   nobs(res_low$models$retail),
        nobs(res_high$models$inst),     nobs(res_low$models$inst)
      ),
      stringsAsFactors = FALSE
    )
  )
  setColWidths(wb, "Sample Sizes", cols = 1:2, widths = c(34, 12))
  
  saveWorkbook(
    wb,
    file.path(OUT_DIR_RUN, paste0(FILE_STEM, ".xlsx")),
    overwrite = TRUE
  )
  
  cat("\n===================== COMBINED FEE TABLE BUILT =====================\n")
  cat("Output folder:\n", OUT_DIR_RUN, "\n\n")
  cat("Files created:\n")
  cat(" - ", paste0(FILE_STEM, ".html"), "\n", sep = "")
  cat(" - ", paste0(FILE_STEM, ".tex"), "\n", sep = "")
  cat(" - ", paste0(FILE_STEM, ".xlsx"), "\n", sep = "")
  cat(" - ", paste0(FILE_STEM, ".csv"), "\n\n", sep = "")
}

# ============================================================
# RUN: one combined table for each winsorization version
# ============================================================

# without winsorization
res_high_nw <- fit_fee_models(dtR, dtI, dtC, fee_group = "high")
res_low_nw  <- fit_fee_models(dtR, dtI, dtC, fee_group = "low")
z_tests_nw  <- compute_z_tests(res_high_nw, res_low_nw)

export_combined_fee_table(
  res_high = res_high_nw,
  res_low  = res_low_nw,
  z_tests  = z_tests_nw,
  out_subdir = "without_winsorization"
)

# with winsorization
res_high_w <- fit_fee_models(dtR_win, dtI_win, dtC_win, fee_group = "high")
res_low_w  <- fit_fee_models(dtR_win, dtI_win, dtC_win, fee_group = "low")
z_tests_w  <- compute_z_tests(res_high_w, res_low_w)

export_combined_fee_table(
  res_high = res_high_w,
  res_low  = res_low_w,
  z_tests  = z_tests_w,
  out_subdir = "with_winsorization"
)