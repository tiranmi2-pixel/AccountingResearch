# ===================== 03_alphas_market_adjusted_returns.R =====================
# PURPOSE:
#  1) Add FF factors to dt (rf, mktrf, smb, hml, umd)
#  2) exret
#  3) Rolling CAPM & Carhart alphas (12/24/36)
#  4) Trailing compounded returns (fund + market, 12/60/120) and market-adjusted excess
#  5) Auto-run Script 04
#
# RULE:
#  - Preserve dt's existing column order.
#  - Only append new columns to the far right.

library(data.table)

stopifnot(exists("dt"), exists("ff"), exists("mkt_idx_df"), exists("proj_root"))
setDT(dt)
old_cols <- names(dt)

# Ensure types
dt[, caldt := as.Date(caldt)]
dt[, crsp_fundno := as.numeric(crsp_fundno)]
dt[, mret := as.numeric(mret)]
dt[, mtna := as.numeric(mtna)]
if (is.finite(mean(abs(dt$mret), na.rm=TRUE)) && mean(abs(dt$mret), na.rm=TRUE) > 1) dt[, mret := mret/100]
setorder(dt, crsp_fundno, caldt)

# -----------------------
# A) Add FF factors by update-join (no column reorder)
# -----------------------
ff_dt <- copy(as.data.table(ff))
ff_dt[, caldt := as.Date(caldt)]
setkey(ff_dt, caldt)

# Add factor columns (only if not already present)
for (v in c("rf","mktrf","smb","hml","umd")) {
  if (!(v %in% names(dt))) dt[, (v) := NA_real_]
}

setkey(dt, caldt)
dt[ff_dt, `:=`(
  rf    = i.rf,
  mktrf = i.mktrf,
  smb   = i.smb,
  hml   = i.hml,
  umd   = i.umd
), on = .(caldt)]

# exret
if (!("exret" %in% names(dt))) dt[, exret := as.numeric(mret) - as.numeric(rf)]

# -----------------------
# B) Monthly flow (kept here so controls can reuse)
#    (no reordering; just append)
# -----------------------
if (!("mtna_l1" %in% names(dt))) dt[, mtna_l1 := shift(mtna, 1L), by = crsp_fundno]
if (!("flow" %in% names(dt))) {
  dt[, flow := (mtna - mtna_l1 * (1 + mret)) / mtna_l1]
  dt[is.na(mtna_l1) | mtna_l1 <= 0, flow := NA_real_]
}
if (!("flow_l1" %in% names(dt))) dt[, flow_l1 := shift(flow, 1L), by = crsp_fundno]

# -----------------------
# C) Rolling alphas (same logic)
# -----------------------
roll_alpha_dt <- function(y_vec, X_mat, width) {
  n <- length(y_vec)
  if (n < width) return(rep(NA_real_, n))
  
  idx <- seq_len(n)
  data.table::frollapply(
    X     = idx,
    N     = width,
    align = "right",
    fill  = NA_real_,
    FUN   = function(w_idx) {
      yy <- y_vec[w_idx]
      XX <- X_mat[w_idx, , drop = FALSE]
      
      ok <- is.finite(yy) & apply(XX, 1, function(r) all(is.finite(r)))
      if (sum(ok) < width) return(NA_real_)
      
      fit <- lm.fit(x = cbind(1, XX[ok, , drop = FALSE]), y = yy[ok])
      as.numeric(fit$coefficients[1])
    }
  )
}

dt[, c("alpha_capm_12m","alpha_capm_24m","alpha_capm_36m",
       "alpha_carhart_12m","alpha_carhart_24m","alpha_carhart_36m") := {
         y_vec <- exret
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

# -----------------------
# D) Trailing returns (same logic)
# -----------------------
idx <- as.data.table(mkt_idx_df)
idx[, caldt := as.Date(caldt)]
idx[, mkt_ret := as.numeric(mkt_ret)]
setorder(idx, caldt)

idx[, `:=`(
  mkt_ret_12m  = exp(frollsum(log1p(mkt_ret),  12L, align = "right")) - 1,
  mkt_ret_60m  = exp(frollsum(log1p(mkt_ret),  60L, align = "right")) - 1,
  mkt_ret_120m = exp(frollsum(log1p(mkt_ret), 120L, align = "right")) - 1
)]

# Add market series via update-join (no reorder)
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

# Fund trailing returns
dt[, `:=`(
  fund_ret_12m  = exp(frollsum(log1p(mret),  12L, align = "right")) - 1,
  fund_ret_60m  = exp(frollsum(log1p(mret),  60L, align = "right")) - 1,
  fund_ret_120m = exp(frollsum(log1p(mret), 120L, align = "right")) - 1
), by = crsp_fundno]

# Market-adjusted
dt[, `:=`(
  excess_ret_12m  = fund_ret_12m  - mkt_ret_12m,
  excess_ret_60m  = fund_ret_60m  - mkt_ret_60m,
  excess_ret_120m = fund_ret_120m - mkt_ret_120m
)]

# -----------------------
# E) Preserve original order; append new columns to the far right
# -----------------------
new_cols <- setdiff(names(dt), old_cols)
setcolorder(dt, c(old_cols, new_cols))

assign("dt", dt, envir = .GlobalEnv)

cat("\n[03] Alphas + trailing returns added.\n")
cat("[03] Rows:", nrow(dt), " Cols:", ncol(dt), "\n")
cat("[03] Non-missing alpha_carhart_36m:", sum(!is.na(dt$alpha_carhart_36m)), "\n")

# Auto-run Script 04
source(file.path(proj_root, "Scripts", "04_controls_and_save.R"))
