# ===================== TSR WINDOW COVERAGE: CLASS-LEVEL HEATMAP + SUMMARY =====================
# PURPOSE:
#   Show how many share classes (crsp_fundno) have:
#     - full 24 months pre and post
#     - less than 24 months on one or both sides
#   and visualize the exact pre/post month combinations in a heatmap.
#
# INPUT:
#   dt_24 must exist in memory
#
# OUTPUT:
#   1) class_window_summary      -> one row per class
#   2) coverage_counts_table     -> summary counts
#   3) heatmap of pre_months x post_months
#   4) bar chart of coverage groups
#
# NOTE:
#   This is CLASS-LEVEL (crsp_fundno).
#   If you want PORTFOLIO-LEVEL instead, replace crsp_fundno with crsp_portno.
# ========================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
  library(scales)
})

stopifnot(exists("dt_24", envir = .GlobalEnv))

DT <- copy(as.data.table(get("dt_24", envir = .GlobalEnv)))

# -----------------------
# 1) Basic date hygiene
# -----------------------
DT[, caldt := as.Date(caldt)]
DT[, tsr_filingdate_first_mend := as.Date(tsr_filingdate_first_mend)]

# Drop rows missing essential fields
DT <- DT[!is.na(crsp_fundno) & !is.na(caldt) & !is.na(tsr_filingdate_first_mend)]

# -----------------------
# 2) Build one-row-per-class summary
# -----------------------
# We count UNIQUE months before and after the class's TSR month-end anchor.
class_window_summary <- DT[, .(
  first_month = min(caldt, na.rm = TRUE),
  last_month  = max(caldt, na.rm = TRUE),
  n_rows      = .N,
  n_months    = uniqueN(caldt),
  pre_months  = uniqueN(caldt[caldt <  tsr_filingdate_first_mend]),
  post_months = uniqueN(caldt[caldt >= tsr_filingdate_first_mend]),
  crsp_portno = if (uniqueN(crsp_portno, na.rm = TRUE) == 1) unique(na.omit(crsp_portno))[1] else NA_real_
), by = crsp_fundno]

# Cap at 24 for cleaner interpretation if desired
# (dt_24 should already be within +/-24 months, but this is just a safeguard)
class_window_summary[, pre_months  := pmin(pre_months, 24L)]
class_window_summary[, post_months := pmin(post_months, 24L)]

# -----------------------
# 3) Coverage group labels
# -----------------------
class_window_summary[, coverage_group := fifelse(
  pre_months == 24 & post_months == 24, "Full 24 pre + 24 post",
  fifelse(
    pre_months == 24 & post_months < 24, "Full pre, incomplete post",
    fifelse(
      pre_months < 24 & post_months == 24, "Incomplete pre, full post",
      "Incomplete pre and post"
    )
  )
)]

class_window_summary[, coverage_group := factor(
  coverage_group,
  levels = c(
    "Full 24 pre + 24 post",
    "Full pre, incomplete post",
    "Incomplete pre, full post",
    "Incomplete pre and post"
  )
)]

# -----------------------
# 4) Summary count table
# -----------------------
coverage_counts_table <- class_window_summary[, .(
  n_classes = .N,
  pct_classes = 100 * .N / nrow(class_window_summary)
), by = coverage_group][order(coverage_group)]

print(coverage_counts_table)

cat("\nTotal classes:", nrow(class_window_summary), "\n")
cat("Classes with full 24/24 window:",
    class_window_summary[pre_months == 24 & post_months == 24, .N], "\n")

# -----------------------
# 5) Heatmap data
# -----------------------
heat_dt <- class_window_summary[, .N, by = .(pre_months, post_months)]

# Fill all 0:24 combinations so the heatmap has a full grid
full_grid <- CJ(pre_months = 0:24, post_months = 0:24)
heat_dt <- heat_dt[full_grid, on = .(pre_months, post_months)]
heat_dt[is.na(N), N := 0L]

# -----------------------
# 6) Heatmap
# -----------------------
p_heat <- ggplot(heat_dt, aes(x = pre_months, y = post_months, fill = N)) +
  geom_tile(color = "white", linewidth = 0.25) +
  geom_text(aes(label = ifelse(N > 0, N, "")), size = 3) +
  scale_x_continuous(breaks = seq(0, 24, by = 2)) +
  scale_y_continuous(breaks = seq(0, 24, by = 2)) +
  scale_fill_gradient(low = "grey95", high = "navy", labels = comma) +
  labs(
    title = "Class-level TSR event-window coverage",
    subtitle = "Each tile shows the number of share classes with a given number of pre- and post-TSR months",
    x = "Months available before TSR month-end",
    y = "Months available after TSR month-end",
    fill = "No. of classes"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

print(p_heat)

# -----------------------
# 7) Summary bar chart
# -----------------------
p_bar <- ggplot(coverage_counts_table, aes(x = coverage_group, y = n_classes)) +
  geom_col() +
  geom_text(aes(label = paste0(n_classes, "\n(", round(pct_classes, 1), "%)")),
            vjust = -0.2, size = 4) +
  labs(
    title = "Coverage of TSR event windows across share classes",
    x = NULL,
    y = "Number of classes"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 20, hjust = 1)
  )

print(p_bar)

# -----------------------
# 8) Optional: exact frequency table for appendix / export
# -----------------------
pre_post_table <- dcast(
  heat_dt,
  post_months ~ pre_months,
  value.var = "N",
  fill = 0
)

print(pre_post_table)

# -----------------------
# 9) Optional exports
# -----------------------
if (exists("proj_root", envir = .GlobalEnv)) {
  out_dir <- file.path(get("proj_root", envir = .GlobalEnv), "Descriptive Statistics")
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  fwrite(class_window_summary,
         file.path(out_dir, "class_window_summary_dt24.csv"))
  
  fwrite(coverage_counts_table,
         file.path(out_dir, "class_window_coverage_counts_dt24.csv"))
  
  fwrite(pre_post_table,
         file.path(out_dir, "class_window_heatmap_table_dt24.csv"))
  
  ggsave(
    filename = file.path(out_dir, "class_window_heatmap_dt24.png"),
    plot = p_heat, width = 10, height = 8, dpi = 300
  )
  
  ggsave(
    filename = file.path(out_dir, "class_window_coverage_bar_dt24.png"),
    plot = p_bar, width = 10, height = 6, dpi = 300
  )
  
  cat("\nSaved outputs to:", out_dir, "\n")
}