# ===================== 06_export_final_samples.R =====================
# PURPOSE:
#  1) Save full enriched equity-filtered dt (full history) to CSV
#  2) Create and save one restricted sample:
#       A) Event window: 24 months pre/post around tsr_filingdate_first -> dt_24
#  3) Keep dt_24 in memory for downstream analysis
# ===============================================================

library(data.table)
library(lubridate)

# ---- Preconditions: these should exist if you run after Script 04 ----
stopifnot(exists("dt"), exists("proj_root"))
DT <- copy(get("dt", envir = .GlobalEnv))
setDT(DT)

# ---- Basic date hygiene ----
if (!("caldt" %in% names(DT))) stop("DT is missing caldt.")
DT[, caldt := as.Date(caldt)]

# ---- Output folder ----
out_dir <- file.path(proj_root, "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# ===============================================================
# 1) Save FULL enriched equity-filtered panel (full history)
# ===============================================================
full_path <- file.path(out_dir, "Full_equity_filtered_data.csv")
fwrite(DT, full_path)
cat("[07] Saved FULL equity-filtered enriched dt to:\n", full_path, "\n", sep="")


# ===============================================================
# 2) Define eligible universe first: funds observed in 2022-2025
# ===============================================================
ELIG_START <- as.Date("2022-01-01")
ELIG_END   <- as.Date("2025-12-31")

# Funds that are actually present in the policy-era window
eligible_portnos <- unique(
  DT[caldt >= ELIG_START & caldt <= ELIG_END & !is.na(crsp_portno), crsp_portno]
)


cat("[07] Eligible portfolios observed in 2022-2025:", length(eligible_portnos), "\n")

# Keep ALL rows for those eligible funds (not just 2022-2025 rows)
DT_base <- DT[crsp_portno %in% eligible_portnos]

cat("[07] Rows in DT_base (all months for eligible funds):", nrow(DT_base), "\n")

# ===============================================================
# 3) Event-window sample: +/- 24 months around tsr_filingdate_first
# ===============================================================
tsr_col <- "tsr_filingdate_first"

if (!(tsr_col %in% names(DT_base))) {
  stop(
    "DT_base does not contain '", tsr_col, "'.\n",
    "If your pipeline uses a different name, change tsr_col in this script."
  )
}

DT_base[, tsr_filingdate_first := as.Date(tsr_filingdate_first)]

# Align to month-end because caldt is monthly
DT_base[, tsr_filingdate_first_mend := ceiling_date(get(tsr_col), unit = "month") - days(1)]

# Earliest non-missing TSR filing date per fund
ev <- DT_base[!is.na(tsr_filingdate_first_mend),
              .(tsr_filingdate_first_mend = min(tsr_filingdate_first_mend, na.rm = TRUE)),
              by = crsp_portno]

ev[, win_start := tsr_filingdate_first_mend %m-% months(24)]
ev[, win_end   := tsr_filingdate_first_mend %m+% months(24)]

setkey(ev, crsp_portno)
dt_24 <- ev[DT_base, on = "crsp_portno"]
# Keep only observations inside each fund's event window
dt_24 <- dt_24[caldt >= win_start & caldt <= win_end]

# Optional helper diagnostics
dt_24[, pre_months := uniqueN(caldt[caldt < tsr_filingdate_first_mend]), by = crsp_portno]
dt_24[, post_months := uniqueN(caldt[caldt >= tsr_filingdate_first_mend]), by = crsp_portno]

# Drop helper cols you do not want
dt_24[, c("win_start", "win_end") := NULL]

setorder(dt_24, crsp_portno, crsp_fundno, caldt)

path_24 <- file.path(out_dir, "Equity_filtered_24m_pre_post_by_tsr_filingdate_first.csv")
fwrite(dt_24, path_24)

cat("[07] Saved +/-24m event-window dt_24 to:\n", path_24, "\n", sep = "")
cat("[07] dt_24 rows:", nrow(dt_24), "\n")
cat("[07] dt_24 unique funds:", uniqueN(dt_24$crsp_fundno), "\n")

assign("dt_24", dt_24, envir = .GlobalEnv)


window_check <- dt_24[, .(
  first_month = min(caldt, na.rm = TRUE),
  last_month  = max(caldt, na.rm = TRUE),
  n_rows      = .N,
  n_months    = uniqueN(caldt),
  pre_months  = uniqueN(caldt[caldt < tsr_filingdate_first_mend]),
  post_months = uniqueN(caldt[caldt >= tsr_filingdate_first_mend])
), by = crsp_portno]

print(summary(window_check$n_months))
print(summary(window_check$pre_months))
print(summary(window_check$post_months))

# -----------------------
# AUTO-RUN SCRIPT 07
# -----------------------
script7 <- file.path(proj_root, "Scripts", "07_Analysis.R")
if (!file.exists(script7)) stop("Script 07 not found at: ", script7)
source(script7)