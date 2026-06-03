# ===================== MISSINGNESS TABLE FOR dt_24 =====================
# PURPOSE:
#   Build a variable-level missingness table like:
#   dataset | group | column | present_in_dt | Total | non_missing | Missing No | Percentage missing
#
# INPUT:
#   dt_24 must already exist in memory
#
# OUTPUT:
#   missing_table_dt24  (in memory)
#   Optional CSV + Excel export
# ================================================================

library(data.table)
library(openxlsx)

stopifnot(exists("dt_24", envir = .GlobalEnv))

DT <- as.data.table(copy(get("dt_24", envir = .GlobalEnv)))
dataset_name <- "dt_24"

# -----------------------
# 1) Define groups/columns
# -----------------------
var_groups <- list(
  "id/core" = c(
    "crsp_fundno",
    "crsp_portno",
    "caldt",
    "retail_fund",
    "inst_fund"
  ),
  "dependent" = c(
    "flow",
    "flow_l1"
  ),
  "independent" = c(
    "alpha_capm_12m",
    "alpha_capm_24m",
    "alpha_capm_36m",
    "alpha_carhart_12m",
    "alpha_carhart_24m",
    "alpha_carhart_36m",
    "fund_ret_12m",
    "fund_ret_60m",
    "fund_ret_120m",
    "excess_ret_12m",
    "excess_ret_60m",
    "excess_ret_120m"
  ),
  "controls" = c(
    "age_years",
    "age_years_l1",
    "mtna",
    "mtna_l1",
    "log_tna_l1",
    "family_tna_l1",
    "log_familytna_l1",
    "flow_l1",
    "turn_ratio",
    "turn_ratio_l1",
    "mgmt_fee",
    "mgmt_fee_l1",
    "exp_ratio",
    "exp_ratio_l1"
  
  ),
  "tsr_dates" = c(
    "tsr_filingdate_first_mend",
    "tsr_filingdate_first",
    "tsr_reportdate_first"
  )
)

# -----------------------
# 2) Helper to summarize one variable
# -----------------------
summ_one <- function(DT, var, grp, dataset_name) {
  total_n <- nrow(DT)
  present <- var %in% names(DT)
  
  if (!present) {
    return(data.table(
      dataset = dataset_name,
      group = grp,
      column = var,
      present_in_dt = FALSE,
      Total = total_n,
      non_missing = NA_integer_,
      `Missing No` = NA_integer_,
      `Percentage missing` = NA_real_
    ))
  }
  
  non_missing_n <- sum(!is.na(DT[[var]]))
  missing_n <- total_n - non_missing_n
  pct_missing <- round(100 * missing_n / total_n, 3)
  
  data.table(
    dataset = dataset_name,
    group = grp,
    column = var,
    present_in_dt = TRUE,
    Total = total_n,
    non_missing = non_missing_n,
    `Missing No` = missing_n,
    `Percentage missing` = pct_missing
  )
}

# -----------------------
# 3) Build the table
# -----------------------
missing_table_dt24 <- rbindlist(
  lapply(names(var_groups), function(grp) {
    rbindlist(
      lapply(var_groups[[grp]], function(v) {
        summ_one(DT, v, grp, dataset_name)
      }),
      fill = TRUE
    )
  }),
  fill = TRUE
)

# -----------------------
# 4) Print in console
# -----------------------
print(missing_table_dt24)

# -----------------------
# 5) Exports -> Descriptive Statistics folder inside project directory
# -----------------------
desc_dir <- file.path(proj_root, "Descriptive Statistics")
dir.create(desc_dir, recursive = TRUE, showWarnings = FALSE)

# CSV
fwrite(
  missing_table_dt24,
  file.path(desc_dir, "missing_table_dt24.csv")
)

# Excel
wb <- createWorkbook()
addWorksheet(wb, "missing_dt24")
writeData(wb, "missing_dt24", missing_table_dt24)
saveWorkbook(
  wb,
  file.path(desc_dir, "missing_table_dt24.xlsx"),
  overwrite = TRUE
)


