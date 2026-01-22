library(data.table)
library(fixest)

# base is your prepared panel with:
# crsp_portno, crsp_fundno, ym, retail (0/1), flow, approx_tsr_dis_monthend, etc.

# portfolio-level event month (you already computed this as port_event_mend)
# ensure time variable is ym (integer YYYYMM) and cohort is also integer YYYYMM
base[, event_ym := as.integer(format(port_event_mend, "%Y%m"))]

pv <- "alpha_capm_12m"   # loop over PERF_VARS

D <- copy(base)
D[, perf := as.numeric(get(pv))]
D[, perf_l1 := shift(perf, 1L), by = crsp_fundno]
D <- D[is.finite(flow) & is.finite(perf_l1) & !is.na(event_ym)]

# dynamic: Perf × Retail × event-time bins (sunab builds relative-time dummies)
m_es <- feols(
  flow ~ perf_l1 * retail * sunab(event_ym, ym, ref.p = -1) | crsp_portno + ym,
  data = D,
  cluster = ~crsp_portno
)

# Plot the dynamic coefficients for perf_l1:retail × event time
iplot(m_es, ref.line = 0)

pv <- "alpha_capm_12m"

D <- copy(base)
D[, perf := as.numeric(get(pv))]
D[, perf_l1 := shift(perf, 1L), by = crsp_fundno]
D <- D[is.finite(flow) & is.finite(perf_l1) & !is.na(post_event)]

m_within_month <- feols(
  flow ~ perf_l1 * post_event * retail | crsp_portno^ym + crsp_fundno,
  data = D,
  cluster = ~crsp_portno
)

etable(m_within_month)
