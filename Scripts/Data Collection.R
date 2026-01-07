#============LOAD LIABRIES & ESTABLISH CONNECTION==========================
#==========================================================================
# Load libraries
library(RPostgres)
library(dplyr)
library(DBI)
library(lubridate)
library(openxlsx)



# Wrds connection
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='tiran')




#====================EXTRACT DATA============================================
#============================================================================


# Step 1- Extracting monthly fund panel (returns + NAV + TNA)
mf_monthly_df <- dbGetQuery(wrds, "
  SELECT crsp_fundno, caldt, mret, mnav, mtna
  FROM crsp.monthly_tna_ret_nav
  WHERE caldt BETWEEN '2015-01-01' AND '2026-12-31'
")
mf_monthly_df$caldt <- as.Date(mf_monthly_df$caldt)

# Step 2 - Connect share class records to the portfolio level fund identifier.
map_df <- dbGetQuery(wrds, "
  SELECT crsp_fundno, crsp_portno, begdt, enddt,
         crsp_cl_grp,retail_fund,inst_fund,
        fund_name,ticker,index_fund_flag,et_flag,vau_fund,dead_flag,
        cusip8, ncusip, first_offer_dt,
         mgmt_name, mgmt_cd,
         mgr_name, mgr_dt,
         adv_name, open_to_inv,
         m_fund,
         end_dt, delist_cd, merge_fundno
  
  FROM crsp.portnomap
")

contact_df <- dbGetQuery(wrds, "
  SELECT crsp_fundno, chgdt, chgenddt, city, state, website
  FROM crsp.contact_info
")

contact_df$chgdt    <- as.Date(contact_df$chgdt)
contact_df$chgenddt <- as.Date(contact_df$chgenddt)
map_df$begdt <- as.Date(map_df$begdt)
map_df$enddt <- as.Date(map_df$enddt)
map_df$first_offer_dt <- as.Date(map_df$first_offer_dt)
map_df$mgr_dt <- as.Date(map_df$mgr_dt)
map_df$end_dt <- as.Date(map_df$end_dt)



# Step 3 -  Merge everything together.
#   1) joins share-class data to the mapping table
#   2) keeps only mappings valid in that month (caldt in [begdt, enddt])
#   3) if multiple mappings match, keeps the most recent one (latest begdt)
#   4) attaches fund_name and ticker
mf_with_names <- mf_monthly_df %>%
  left_join(map_df, by = "crsp_fundno") %>%
  # Recommended small robustness tweak: keep rows where enddt is NULL (still active)
  filter(caldt >= begdt & (is.na(enddt) | caldt <= enddt)) %>%
  group_by(crsp_fundno, caldt) %>%
  slice_max(begdt, n = 1, with_ties = FALSE) %>%
  ungroup()


# Step 4 - Attach contact info valid for that month
mf_with_names <- mf_with_names %>%
  left_join(contact_df, by = "crsp_fundno") %>%
  # keep rows with no contact info, or those within valid contact dates
  filter(is.na(chgdt) | (caldt >= chgdt & (is.na(chgenddt) | caldt <= chgenddt))) %>%
  group_by(crsp_fundno, caldt) %>%
  slice_max(chgdt, n = 1, with_ties = FALSE) %>%
  ungroup()



mf_with_names <- mf_with_names %>%
  select(
    crsp_fundno, crsp_portno, ticker, fund_name,
    city, state, website,                      # you want these early
    crsp_cl_grp, retail_fund, inst_fund,
    caldt, mret, mnav, mtna,
    
    begdt, enddt,
    cusip8, ncusip, first_offer_dt,
    mgmt_name, mgmt_cd,
    mgr_name, mgr_dt,
    adv_name, open_to_inv,
    m_fund,
    index_fund_flag, vau_fund, et_flag,
    end_dt, dead_flag, delist_cd, merge_fundno
  )

 

# Display results of the collected data
summary_tbl <- mf_with_names %>%
  mutate(
    retail_fund = toupper(trimws(retail_fund)),
    inst_fund   = toupper(trimws(inst_fund))
  ) %>%
  summarise(
    unique_funds_portfolio = n_distinct(crsp_portno),   # "funds" at portfolio level
    unique_share_classes   = n_distinct(crsp_fundno),   # total share classes
    
    retail_share_classes = n_distinct(crsp_fundno[retail_fund == "Y" & inst_fund != "Y"]),
    inst_share_classes   = n_distinct(crsp_fundno[inst_fund   == "Y" & retail_fund != "Y"]),
    both_retail_and_inst = n_distinct(crsp_fundno[retail_fund == "Y" & inst_fund == "Y"])
  )

View(summary_tbl)
summary_tbl

#==================== SAVE OUTPUT TO EXCEL (FORMATTED) ====================

# Install if needed
if (!requireNamespace("openxlsx", quietly = TRUE)) install.packages("openxlsx")
library(openxlsx)
library(dplyr)
library(lubridate)

# 1) Output folder inside your project directory
out_dir <- file.path(getwd(), "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_file <- file.path(out_dir, "mf_outputs_2015_2026.xlsx")

# 2) Create workbook
wb <- createWorkbook()

# ---------- Styles ----------
date_style   <- createStyle(numFmt = "yyyy-mm-dd")
pct_style    <- createStyle(numFmt = "0.00%")
money_style  <- createStyle(numFmt = "#,##0.00")
int_style    <- createStyle(numFmt = "#,##0")

# Helper: apply a style to an entire column (excluding header)
apply_style_col <- function(wb, sheet, data, col_name, style) {
  if (!col_name %in% names(data)) return(invisible(NULL))
  col_idx <- match(col_name, names(data))
  n <- nrow(data)
  if (n > 0) {
    addStyle(wb, sheet, style = style, rows = 2:(n + 1), cols = col_idx,
             gridExpand = TRUE, stack = TRUE)
  }
}

# 3) Sheet: summary_tbl
addWorksheet(wb, "summary")
writeDataTable(wb, "summary", summary_tbl, tableStyle = "TableStyleMedium9")
freezePane(wb, "summary", firstRow = TRUE)
setColWidths(wb, "summary", cols = 1:ncol(summary_tbl), widths = "auto")

# 4) Sheet(s): mf_with_names (split if too many rows for one sheet)
excel_row_limit <- 1048576
max_data_rows   <- excel_row_limit - 1  # minus header row

# Function to write a data frame to a sheet with basic formatting
write_formatted_sheet <- function(wb, sheet_name, dat) {
  addWorksheet(wb, sheet_name)
  writeDataTable(wb, sheet_name, dat, tableStyle = "TableStyleMedium2")
  freezePane(wb, sheet_name, firstRow = TRUE)
  addFilter(wb, sheet_name, row = 1, cols = 1:ncol(dat))
  
  # Column widths: "auto" can be slow on huge sheets; use a reasonable fixed width pattern
  setColWidths(wb, sheet_name, cols = 1:ncol(dat), widths = 14)
  
  # Apply formats (only if those columns exist)
  apply_style_col(wb, sheet_name, dat, "caldt", date_style)
  apply_style_col(wb, sheet_name, dat, "begdt", date_style)
  apply_style_col(wb, sheet_name, dat, "enddt", date_style)
  apply_style_col(wb, sheet_name, dat, "first_offer_dt", date_style)
  apply_style_col(wb, sheet_name, dat, "mgr_dt", date_style)
  apply_style_col(wb, sheet_name, dat, "end_dt", date_style)
  
  apply_style_col(wb, sheet_name, dat, "mret", pct_style)
  apply_style_col(wb, sheet_name, dat, "mnav", money_style)
  apply_style_col(wb, sheet_name, dat, "mtna", money_style)
  
  # IDs as integers (optional)
  apply_style_col(wb, sheet_name, dat, "crsp_fundno", int_style)
  apply_style_col(wb, sheet_name, dat, "crsp_portno", int_style)
}

n_total <- nrow(mf_with_names)

if (n_total <= max_data_rows) {
  # Fits in one sheet
  write_formatted_sheet(wb, "mf_with_names", mf_with_names)
} else {
  # Split by year, and chunk further if needed
  mf_with_names <- mf_with_names %>% mutate(year = year(caldt))
  
  years <- sort(unique(mf_with_names$year))
  for (yy in years) {
    dat_y <- mf_with_names %>% filter(year == yy) %>% select(-year)
    n_y <- nrow(dat_y)
    
    if (n_y <= max_data_rows) {
      write_formatted_sheet(wb, paste0("mf_", yy), dat_y)
    } else {
      # Chunk within year
      n_chunks <- ceiling(n_y / max_data_rows)
      for (k in seq_len(n_chunks)) {
        idx_start <- (k - 1) * max_data_rows + 1
        idx_end   <- min(k * max_data_rows, n_y)
        dat_chunk <- dat_y[idx_start:idx_end, , drop = FALSE]
        write_formatted_sheet(wb, paste0("mf_", yy, "_part", k), dat_chunk)
      }
    }
  }
}

# 5) Save
saveWorkbook(wb, out_file, overwrite = TRUE)``

message("Saved Excel file to: ", out_file)




