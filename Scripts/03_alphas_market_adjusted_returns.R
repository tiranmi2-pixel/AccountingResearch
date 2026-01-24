# ===================== 03_alphas_market_adjusted_returns.R =====================
# PURPOSE:
#  1) Add FF factors to dt (rf, mktrf, smb, hml, umd)
#  2) exret
#  3) Rolling CAPM & Carhart alphas (12/24/36)
#  4) Trailing compounded returns (fund + market, 12/60/120) and market-adjusted excess
#  5) Auto-run Script 04

# =====================================================================
# ==== Libraries + pre-flight checks ====
# =====================================================================
library(data.table)

# Script 01 should have created dt + factor objects in memory already
stopifnot(exists("dt"), exists("ff"), exists("mkt_idx_df"), exists("proj_root"))

setDT(dt)

# We'll preserve the original order and only append new columns at the end
old_cols <- names(dt)

# =====================================================================
# ==== Basic type cleanup (so math + rolling ops behave) ====
# =====================================================================
dt[, caldt := as.Date(caldt)]
dt[, crsp_fundno := as.numeric(crsp_fundno)]
dt[, mret := as.numeric(mret)]
dt[, mtna := as.numeric(mtna)]

# Just a safety net: if returns look like percent (avg abs > 1), convert to decimals
if (is.finite(mean(abs(dt$mret), na.rm=TRUE)) && mean(abs(dt$mret), na.rm=TRUE) > 1) dt[, mret := mret/100]

# Keep panel sorted (rolling windows and shifts depend on this)
setorder(dt, crsp_fundno, caldt)

# =====================================================================
# ==== A) Add Fama-French factors via update-join (by caldt) ====
# =====================================================================
ff_dt <- copy(as.data.table(ff))
ff_dt[, caldt := as.Date(caldt)]
setkey(ff_dt, caldt)

# Make sure the factor columns exist before we try to update-join into them
for (v in c("rf","mktrf","smb","hml","umd")) {
  if (!(v %in% names(dt))) dt[, (v) := NA_real_]
}

# Join on month-end date; pulls rf + factor returns into every dt row for that month
setkey(dt, caldt)
dt[ff_dt, `:=`(
  rf    = i.rf,
  mktrf = i.mktrf,
  smb   = i.smb,
  hml   = i.hml,
  umd   = i.umd
), on = .(caldt)]

# Excess return = fund return minus risk-free rate
if (!("exret" %in% names(dt))) dt[, exret := as.numeric(mret) - as.numeric(rf)]

# =====================================================================
# ==== B) Monthly flow (kept here so other scripts/controls can reuse) ====
# =====================================================================
# We compute lagged TNA and fund flow once and keep it on dt.
# Flow formula is the standard CRSP-style: isolate net flows from TNA growth.
if (!("mtna_l1" %in% names(dt))) dt[, mtna_l1 := shift(mtna, 1L), by = crsp_fundno]

if (!("flow" %in% names(dt))) {
  dt[, flow := (mtna - mtna_l1 * (1 + mret)) / mtna_l1]
  # If lagged TNA is missing or non-positive, flow is undefined — set it to NA
  dt[is.na(mtna_l1) | mtna_l1 <= 0, flow := NA_real_]
}

# Lagged flow is handy for regressions (and gets used later in controls)
if (!("flow_l1" %in% names(dt))) dt[, flow_l1 := shift(flow, 1L), by = crsp_fundno]

# =====================================================================
# ==== C) Rolling CAPM / Carhart alphas (windowed regressions) ====
# =====================================================================
# roll_alpha_dt runs a rolling regression and returns the intercept (alpha).
# - width is the window length (e.g., 12/24/36 months)
# - min_obs lets you be tolerant if you want (here default is strict == width)
roll_alpha_dt <- function(y_vec, X_mat, width, min_obs = width) {
  n <- length(y_vec)
  if (n < width) return(rep(NA_real_, n))
  
  # We roll over row indices, then slice y/X inside the rolling function
  idx <- seq_len(n)
  
  data.table::frollapply(
    X     = idx,
    N     = width,
    align = "right",
    fill  = NA_real_,
    FUN   = function(w_idx) {
      yy <- y_vec[w_idx]
      XX <- X_mat[w_idx, , drop = FALSE]
      
      # Keep only rows where y and all regressors are finite
      ok <- is.finite(yy) & apply(XX, 1, function(r) all(is.finite(r)))
      if (sum(ok) < min_obs) return(NA_real_)
      
      # lm.fit is a fast base-R regression; intercept is alpha
      fit <- lm.fit(x = cbind(1, XX[ok, , drop = FALSE]), y = yy[ok])
      as.numeric(fit$coefficients[1])
    }
  )
}

# Compute all rolling alphas per share class, in one grouped assignment
dt[, c("alpha_capm_12m","alpha_capm_24m","alpha_capm_36m",
       "alpha_carhart_12m","alpha_carhart_24m","alpha_carhart_36m") := {
         
         # Dependent variable is monthly excess return
         y_vec <- exret
         
         # CAPM uses just market excess return; Carhart adds SMB/HML/UMD
         X_capm_mat    <- as.matrix(mktrf)
         X_carhart_mat <- as.matrix(cbind(mktrf, smb, hml, umd))
         
         list(
           roll_alpha_dt(y_vec, X_capm_mat,    12L),
           roll_alpha_dt(y_vec, X_capm_mat,    24L),
           roll_alpha_dt(y_vec, X_capm_mat,    36L),
           roll_alpha_dt(y_vec, X_carhart_mat, 12L),
           roll_alpha_dt(y_vec, X_carhart_mat, 24L),
           roll_alpha_dt(y_vec, X_carhart_mat, 36L)
         )
       }, by = crsp_fundno]

# =====================================================================
# ==== Helper: trailing compounded returns (with a minimum-valid-month threshold) ====
# =====================================================================
# We use log1p/exp to compound returns safely, and enforce a "min_obs" rule
# so you can get values even if a couple months are missing (but not too many).
trail_cumret <- function(r, n, min_obs = n) {
  ok <- is.finite(r) & (r > -1)           # log1p only works if r > -1
  lr <- fifelse(ok, log1p(r), NA_real_)   # log returns; NA where missing/invalid
  
  # Rolling sum of log returns gives log of compounded return
  s_lr <- frollsum(lr, n = n, align = "right", na.rm = TRUE)
  
  # Track how many valid months were actually present in the window
  n_ok <- frollsum(ok, n = n, align = "right", na.rm = TRUE)
  
  out <- exp(s_lr) - 1
  out[n_ok < min_obs] <- NA_real_         # threshold: too many missing months -> NA
  out
}

# =====================================================================
# ==== D) Trailing returns: market series + fund series + market-adjusted ====
# =====================================================================

# ---- Build trailing market returns first (single time series) ----
idx <- as.data.table(mkt_idx_df)
idx[, caldt := as.Date(caldt)]
idx[, mkt_ret := as.numeric(mkt_ret)]
setorder(idx, caldt)

# Slightly tolerant thresholds here (e.g., 11/12 months ok for 12m)
idx[, `:=`(
  mkt_ret_12m  = trail_cumret(mkt_ret,  12L, min_obs = 11L),
  mkt_ret_60m  = trail_cumret(mkt_ret,  60L, min_obs = 57L),
  mkt_ret_120m = trail_cumret(mkt_ret, 120L, min_obs = 114L)
)]

# ---- Attach the market series to dt by month (update-join; no reorder) ----
for (v in c("mkt_ret","mkt_ret_12m","mkt_ret_60m","mkt_ret_120m")) {
  if (!(v %in% names(dt))) dt[, (v) := NA_real_]
}

setkey(idx, caldt)
setkey(dt, caldt)

dt[idx, `:=`(
  mkt_ret     = i.mkt_ret,
  mkt_ret_12m = i.mkt_ret_12m,
  mkt_ret_60m = i.mkt_ret_60m,
  mkt_ret_120m= i.mkt_ret_120m
), on = .(caldt)]

# ---- Fund trailing returns (computed within each share class) ----
dt[, `:=`(
  fund_ret_12m  = trail_cumret(mret,  12L, min_obs = 11L),
  fund_ret_60m  = trail_cumret(mret,  60L, min_obs = 57L),
  fund_ret_120m = trail_cumret(mret, 120L, min_obs = 114L)
), by = crsp_fundno]

# ---- Market-adjusted trailing returns ----
dt[, `:=`(
  excess_ret_12m  = fund_ret_12m  - mkt_ret_12m,
  excess_ret_60m  = fund_ret_60m  - mkt_ret_60m,
  excess_ret_120m = fund_ret_120m - mkt_ret_120m
)]

# Quick sanity prints so you can spot if 120m windows are sparse
cat("[03] Non-missing fund_ret_120m:", sum(!is.na(dt$fund_ret_120m)), "\n")
cat("[03] Non-missing mkt_ret_120m :", sum(!is.na(dt$mkt_ret_120m)), "\n")
cat("[03] Non-missing excess_ret_120m:", sum(!is.na(dt$excess_ret_120m)), "\n")

# =====================================================================
# ==== E) Preserve column order (append new columns to far right) ====
# =====================================================================
new_cols <- setdiff(names(dt), old_cols)
setcolorder(dt, c(old_cols, new_cols))

# Save updated dt back into the global env for downstream scripts
assign("dt", dt, envir = .GlobalEnv)

cat("\n[03] Alphas + trailing returns added.\n")
cat("[03] Rows:", nrow(dt), " Cols:", ncol(dt), "\n")
cat("[03] Non-missing alpha_carhart_36m:", sum(!is.na(dt$alpha_carhart_36m)), "\n")

# =====================================================================
# ==== Auto-run Script 04 ====
# =====================================================================
source(file.path(proj_root, "Scripts", "04_controls_and_save.R"))
