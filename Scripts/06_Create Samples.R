# ============================================================
# Create TWO analysis CSVs from:
#   mf_with_names_2015_2026_equity_perf_controls.csv
#
# (A) EVENT WINDOW: 24 months pre + 24 months post TSR (fund-specific)
# (B) CALENDAR WINDOW: fixed 4-year window ending 2025-12-31
#
# Key point:
# - For (A) we filter using the TSR "event month-end" date (preferred: approx_tsr_dis_monthend)
# - For (B) we filter using caldt (monthly panel date)
# ============================================================

library(data.table)
library(lubridate)

# -----------------------
# 0) Input file (EDIT if needed)
# -----------------------
in_file <- "C:/Users/tiran/Desktop/Research Github/AccountingResearch/R Raw Data/mf_with_names_2015_2026_equity_perf_controls.csv"

# Put outputs next to the input file (same folder)
out_dir <- dirname(in_file)

# -----------------------
# 1) Load
# -----------------------
dt <- fread(in_file)

# -----------------------
# 2) Basic requirements
# -----------------------
if (!("caldt" %in% names(dt))) stop("Missing required column: caldt")

# Convert caldt to Date if it isn't already
if (!inherits(dt$caldt, "Date")) dt[, caldt := as.Date(caldt)]

# -----------------------
# 3) Pick the TSR event date column (for the event-window dataset)
# -----------------------
# This script will USE ONE of these (first found):
#   1) approx_tsr_dis_monthend  (best: already month-end)
#   2) approx_tsr_dis           (daily date -> we convert to month-end)
#   3) tsr_event_date           (fallback if you renamed)
#   4) tsr_event_monthend       (fallback if you renamed)
candidate_event_cols <- c(
  "approx_tsr_dis_monthend",
  "approx_tsr_dis",
  "tsr_event_date",
  "tsr_event_monthend"
)

event_col <- candidate_event_cols[candidate_event_cols %in% names(dt)][1]
if (is.na(event_col) || is.null(event_col)) {
  stop(
    "No TSR event date column found.\nExpected one of: ",
    paste(candidate_event_cols, collapse = ", ")
  )
}

message("Event-window filter will use TSR date column: ", event_col)

# Make sure the event column is Date
if (!inherits(dt[[event_col]], "Date")) dt[, (event_col) := as.Date(get(event_col))]

# Create a clean month-end event date to compare against caldt
# - If event_col is already month-end, this keeps it in the same month-end
# - If event_col is daily, this converts it to that month’s end
dt[, tsr_event_monthend_used := get(event_col)]
dt[!is.na(tsr_event_monthend_used),
   tsr_event_monthend_used := ceiling_date(tsr_event_monthend_used, unit = "month") - days(1)]

# ============================================================
# (A) EVENT WINDOW DATASET: ±24 months around TSR month-end
# ============================================================

# Window bounds per row (calendar-month aware)
dt[, pre_start_24m := tsr_event_monthend_used %m-% months(24)]
dt[, post_end_24m  := tsr_event_monthend_used %m+% months(24)]

dt_event_24m <- dt[
  !is.na(tsr_event_monthend_used) &
    caldt >= pre_start_24m &
    caldt <= post_end_24m
]

# Clean helper bounds (keep the event month-end used; it’s useful later)
dt_event_24m[, c("pre_start_24m", "post_end_24m") := NULL]

# Clearly distinguishable output name
out_event_24m <- file.path(
  out_dir,
  "mf_with_names_2015_2026_equity_perf_controls_EVENTWIN_PRE24_POST24_byTSR.csv"
)

fwrite(dt_event_24m, out_event_24m)
message("Saved EVENT-WINDOW dataset: ", out_event_24m)
message("  Rows kept: ", nrow(dt_event_24m))

# ============================================================
# (B) CALENDAR WINDOW DATASET: fixed 4 years ending 2025-12-31
# ============================================================

# Your idea: "4 years from Dec 31st 2025"
# Interpreting that as: 2022-01-01 through 2025-12-31 (inclusive)
cal_end   <- as.Date("2025-12-31")
cal_start <- as.Date("2022-01-01")

dt_cal_4y_end2025 <- dt[caldt >= cal_start & caldt <= cal_end]

out_cal_4y <- file.path(
  out_dir,
  "mf_with_names_2015_2026_equity_perf_controls_CALWIN_2022_2025_end2025-12-31.csv"
)

fwrite(dt_cal_4y_end2025, out_cal_4y)
message("Saved CALENDAR-WINDOW dataset: ", out_cal_4y)
message("  Rows kept: ", nrow(dt_cal_4y_end2025))

# -----------------------
# 4) Tiny recap printed to console
# -----------------------
message("\nDone.")
message("Event-window TSR date column used: ", event_col, " (converted to month-end as tsr_event_monthend_used)")
message("Calendar-window uses: caldt between ", cal_start, " and ", cal_end)
