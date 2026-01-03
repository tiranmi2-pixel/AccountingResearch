#============LOAD LIABRIES & ESTABLISH CONNECTION==========================
#==========================================================================
# Load libraries
library(RPostgres)
library(dplyr)
library(DBI)
library(dplyr)
library(lubridate)




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
  SELECT crsp_fundno, crsp_portno, begdt, enddt
  FROM crsp.portnomap
")
map_df$begdt <- as.Date(map_df$begdt)
map_df$enddt <- as.Date(map_df$enddt)

# Step 3-  Extracting fund name and ticker for each portoflio number
fund_names_df <- dbGetQuery(wrds, "
  SELECT DISTINCT crsp_portno, fund_name, ticker
  FROM crsp.portnomap
") %>%
  distinct(crsp_portno, .keep_all = TRUE)   # ensure one row per portfolio

# Step 4 -  Merge everything together.
#   1) joins share-class data to the mapping table
#   2) keeps only mappings valid in that month (caldt in [begdt, enddt])
#   3) if multiple mappings match, keeps the most recent one (latest begdt)
#   4) attaches fund_name and ticker
mf_with_names <- mf_monthly_df %>%
  left_join(map_df, by = "crsp_fundno") %>%
  filter(caldt >= begdt & caldt <= enddt) %>%                  # keep only valid mappings
  group_by(crsp_fundno, caldt) %>%
  slice_max(begdt, n = 1, with_ties = FALSE) %>%               # prevent duplicate fund-month rows
  ungroup() %>%
  select(-begdt, -enddt) %>%
  left_join(fund_names_df, by = "crsp_portno")

# Check results
dim(mf_monthly_df)
dim(mf_with_names)


mf_with_names <- mf_with_names %>%
select(crsp_fundno, crsp_portno, fund_name, ticker, caldt, mret, mnav, mtna, everything())
head(20)