library(data.table)
library(fixest)

# =====================================================================
# ==== 0) Prep: create an event-time key (YYYYMM) for event-study bins ====
# =====================================================================
# We convert the portfolio event month-end date into an integer like 202408.
# sunab() likes a clean "time index" variable, and YYYYMM is convenient here.
base[, event_ym := as.integer(format(port_event_mend, "%Y%m"))]

# =====================================================================
# ==== 1) Event-study spec: dynamic Perf × Retail effects around the event ====
# =====================================================================
# Pick the performance variable we want to run this block for
pv <- "alpha_capm_12m"   # loop over PERF_VARS

# Work on a copy so we don't accidentally mutate `base`
D <- copy(base)

# Pull the performance series into a generic column (makes formulas reusable)
D[, perf := as.numeric(get(pv))]

# Lag performance by 1 month within share class (crsp_fundno)
# This is the "performance at t-1 predicting flow at t" setup.
D[, perf_l1 := shift(perf, 1L), by = crsp_fundno]

# Keep only rows we can actually use in the regression
# - flow must be finite
# - perf_l1 must be finite (needs at least one prior month)
# - event_ym must exist (otherwise sunab can't place the observation in event time)
D <- D[is.finite(flow) & is.finite(perf_l1) & !is.na(event_ym)]

# Dynamic (event-study) model:
# - sunab(event_ym, ym, ref.p = -1) creates event-time bins relative to the event date,
#   using period -1 as the reference (so coefficients are relative to the month before).
# - We interact that event-time structure with perf_l1 and retail, so we're estimating
#   how the flow–performance slope differs by retail status over event time.
m_es <- feols(
  flow ~ perf_l1 * retail * sunab(event_ym, ym, ref.p = -1) | crsp_portno + ym,
  data = D,
  cluster = ~crsp_portno
)

# Plot the dynamic coefficients for the interacted terms over event time
iplot(m_es, ref.line = 0)

# =====================================================================
# ==== 2) Within-month (two-way FE) spec: Perf × Post-event × Retail ====
# =====================================================================
# Same performance variable again (kept explicit so it's easy to swap in a loop)
pv <- "alpha_capm_12m"

# Fresh copy for the second model (keeps filtering/model-specific vars isolated)
D <- copy(base)

# Grab performance + create the 1-month lag within fund
D[, perf := as.numeric(get(pv))]
D[, perf_l1 := shift(perf, 1L), by = crsp_fundno]

# For this model we just need a clean post-event indicator (plus valid flow/perf_l1)
D <- D[is.finite(flow) & is.finite(perf_l1) & !is.na(post_event)]

# Within-month model:
# - crsp_portno^ym is an interaction FE (portfolio-by-month), i.e., we compare
#   share classes within the same portfolio-month.
# - crsp_fundno FE soaks up time-invariant share-class differences.
# - perf_l1 * post_event * retail estimates how the flow–performance slope changes
#   post-event, and whether that change differs for retail vs non-retail.
m_within_month <- feols(
  flow ~ perf_l1 * post_event * retail | crsp_portno^ym + crsp_fundno,
  data = D,
  cluster = ~crsp_portno
)

# Print the regression table in a clean format
etable(m_within_month)
