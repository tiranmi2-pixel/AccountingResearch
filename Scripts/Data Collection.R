#============LOAD LIABRIES & ESTABLISH CONNECTION==========================
#==========================================================================
# Load libraries
library(RPostgres)
library(dplyr)
library(DBI)
library(lubridate)
library(openxlsx)
library(here)


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

#==================== SAVE mf_with_names TO ONE CSV (NO ROW LIMIT) ====================
if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table")
library(data.table)

# Force NON-OneDrive Desktop project root, then use relative paths from it
proj_root <- normalizePath(
  file.path(Sys.getenv("USERPROFILE"),
            "Desktop", "Research Github", "AccountingResearch"),
  winslash = "/", mustWork = TRUE
)

out_dir <- file.path(proj_root, "R Raw Data")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

csv_file <- file.path(out_dir, "mf_with_names_2015_2026.csv")

data.table::fwrite(
  x = mf_with_names,
  file = csv_file,
  sep = ",",
  quote = TRUE,
  na = "",
  bom = TRUE
)

cat("Project root:", proj_root, "\n")
cat("Saved CSV to :", csv_file, "\n")
stopifnot(file.exists(csv_file))