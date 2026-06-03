# AccountingResearch

This repository contains the code for my  research on **Tailored Shareholder Reports (TSR)** and mutual fund investor behavior.

The project builds a monthly CRSP mutual fund share-class panel, identifies when each fund first appears to adopt the TSR format, and studies whether investor flows become more sensitive to past performance after TSR adoption. The analysis focuses on U.S. domestic equity mutual funds and compares overall, retail, and institutional investor responses.

The code is written mainly in **R**, with a separate **Python EDGAR downloader and machine-learning classifier** used to obtain the first TSR filing and report dates.

---

## 1. Project Overview

The main research question is whether the introduction of Tailored Shareholder Reports made mutual fund investors more responsive to fund performance.

The basic logic of the project is:

1. Build a CRSP mutual fund share-class-month panel.
2. Filter the sample to U.S. domestic equity mutual funds.
3. Download N-CSR and N-CSR/A filings separately from EDGAR.
4. Use a machine-learning classifier to identify TSR-format filings.
5. Attach the first TSR filing and report date to each CRSP portfolio.
6. Create a fund-specific pre/post TSR event window.
7. Estimate whether flow-performance sensitivity changes after TSR adoption.

The final empirical sample is a share-class-month panel. TSR adoption is measured at the **portfolio level** using `crsp_portno`, while flows and performance are measured at the **share-class level** using `crsp_fundno`.

---

## 2. Important Note on TSR Dates

The current R pipeline does **not** rely only on WRDS SEC form metadata to infer TSR dates.

Instead, the current workflow uses a separate EDGAR downloader and machine-learning process.

### 2.1 TSR-Date Workflow

1. `05_filter_list_for_downloader.R` creates the downloader input file:

   ```text
   R Raw Data/TSR Manual Search List - Equity.csv
   ```

2. That file is used by a separate Python downloader to collect N-CSR and N-CSR/A filings from EDGAR.

3. The downloader saves both:
   - HTML filing files
   - rendered PDF versions of the filings

4. A machine-learning classifier is then used to detect whether a filing is in TSR format.

5. The ML output is saved as:

   ```text
   Distrbution Dates/tsr_first_tsr_dates_by_fund_ML_FIXED.csv
   ```

6. `05_filter_list_for_downloader.R` reads the ML output and attaches the first TSR dates back to the full R dataset.

### 2.2 TSR-Date Variables

The ML output is attached to the R dataset using the following fields:

```text
tsr_filingdate_first
tsr_reportdate_first
tsr_cik_first
tsr_accession_first
```

The main event date used in the analysis is the first TSR filing date, converted to month-end:

```text
tsr_filingdate_first_mend
```

The fund-specific post indicator is then defined as:

```text
post_fund = 1 if caldt >= tsr_filingdate_first_mend
```

This means each portfolio has its own TSR adoption month.

---
## 3. Repository Structure

The main project folder is now more streamlined. The core R pipeline scripts are stored directly inside the `Scripts/` folder. The deeper regression and descriptive-statistics scripts are stored in `Scripts/Indepth Analysis/`, while the EDGAR downloader and ML-related scripts are stored in `Scripts/Downloader & ML/`.

```text
AccountingResearch/
│
├─ .git/
├─ .Rproj.user/
│
├─ Distribution Dates/
│  └─ tsr_first_tsr_dates_by_fund_ML_FIXED.csv
│
├─ Scripts/
│  ├─ Data Collection.R
│  ├─ 02_equity_filter.R
│  ├─ 03_alphas_market_adjusted_returns.R
│  ├─ 04_controls_and_save.R
│  ├─ 05_filter_list_for_downloader.R
│  ├─ 06_Create Samples.R
│  ├─ 07_Analysis.R
│  │
│  ├─ Downloader & ML/
│  │  ├─ Edgar Downloader.R
│  │  ├─ ML Training - PDF Files.R
│  │  ├─ ML training- HTML files.R
│  │  └─ TSR Detection Using ML.R
│  │
│  └─ Indepth Analysis/
│     ├─ complete_combined_descriptive_statistics_winsorized.R
│     ├─ Cross Sec Fee- Darendeli.R
│     ├─ Cross Sec Fee- Darendeli-Combined.R
│     ├─ Cross Sec Fee- Inflows.R
│     ├─ Cross Sec Fee- Outflows.R
│     ├─ Main- Inflows.R
│     ├─ Main- Outflows.R
│     ├─ Main-Darendelis Flows.R
│     ├─ Quartiles- Darendelis Flows.R
│     ├─ Quartiles- Inflows.R
│     ├─ Quartiles- Outflows.R
│     ├─ Terciles- Darendelis Flows.R
│     ├─ Terciles- Inflows.R
│     └─ Terciles- Outflows.R
│
├─ .gitignore
├─ .Rhistory
├─ AccountingResearch.Rproj
└─ README.md
```

> **Note:** The files inside `Scripts/Downloader & ML/` are Python-based workflow scripts saved with `.R` extensions only so they can be kept with the rest of the project scripts. They are included for transparency about how EDGAR filings were downloaded, how the TSR classifier was trained, and how first TSR dates were identified.
## 4. Main R Pipeline

The main pipeline is designed to run sequentially. Each script creates or enriches the main object `dt`, which is stored in memory as a `data.table`.

---

### 4.1 `Data Collection.R`

This script starts the full WRDS data construction process.

It:

- connects to WRDS using an auto-reconnect wrapper;
- pulls CRSP monthly fund return, NAV, and TNA data from `crsp.monthly_tna_ret_nav`;
- pulls Fama-French monthly factors from `ff.factors_monthly`;
- creates the market return series `mkt_idx_df`;
- attaches CRSP portfolio mappings from `crsp.portnomap`;
- attaches contact information from `crsp.contact_info`;
- attaches style fields such as `lipper_asset_cd`, `policy`, and `fiscal_yearend`;
- attaches CIK information from `crsp_q_mutualfunds.crsp_cik_map`;
- attaches class-level expense ratios from `crsp_q_mutualfunds.fund_fees`;
- creates the core object `dt`;
- auto-runs `02_equity_filter.R`.

The base CRSP pull covers:

```text
2005-01-01 to 2026-12-31
```

The current workflow sets:

```r
RUN_TSR_DATES <- FALSE
```

This matters because TSR dates are now obtained using the separate downloader and ML classifier, not the older WRDS-only approximate-distribution-date logic.

---

### 4.2 `02_equity_filter.R`

This script filters the CRSP mutual fund universe to the main domestic equity sample.

It:

- attaches CRSP objective code and Lipper asset code using an as-of style join;
- removes ETFs and ETNs;
- removes index funds;
- keeps domestic equity funds using CRSP objective codes beginning with `ED`;
- uses Lipper equity classification as a validation layer;
- saves an equity-filter diagnostic table.

Main diagnostic output:

```text
R Raw Data/filter_drop_summary_2022_2025_with_lipper_match.csv
```

This diagnostic file is used to understand how many funds are lost at each filtering step. It is an information and validation output, not a regression output.

After filtering, the script auto-runs:

```text
03_alphas_market_adjusted_returns.R
```

---

### 4.3 `03_alphas_market_adjusted_returns.R`

This script adds performance and return measures to the equity-filtered panel.

It:

- merges Fama-French factors onto each fund-month;
- computes monthly excess returns;
- computes Darendeli-style return-adjusted net flow;
- creates `flow_l1`;
- estimates rolling CAPM alphas over 12, 24, and 36 months;
- estimates rolling Carhart four-factor alphas over 12, 24, and 36 months;
- computes trailing compounded fund returns over 12, 60, and 120 months;
- computes trailing market returns over the same windows;
- computes market-adjusted trailing returns.

Monthly excess return is defined as:

```text
exret = mret - rf
```

Darendeli-style return-adjusted net flow is defined as:

```text
flow = (TNA_t - TNA_{t-1}(1 + R_t)) / (TNA_{t-1}(1 + R_t))
```

Main performance variables created include:

```text
alpha_capm_12m
alpha_capm_24m
alpha_capm_36m
alpha_carhart_12m
alpha_carhart_24m
alpha_carhart_36m
fund_ret_12m
fund_ret_60m
fund_ret_120m
excess_ret_12m
excess_ret_60m
excess_ret_120m
```

The script then auto-runs:

```text
04_controls_and_save.R
```

---

### 4.4 `04_controls_and_save.R`

This script adds control variables and saves the main 2022-2025 equity panel.

It keeps the original column order and appends new control variables to the right.

Controls created or attached include:

```text
age_years
age_years_l1
log_tna_l1
mtna_l1
flow_l1
family_tna_l1
log_familytna_l1
turn_ratio
turn_ratio_l1
mgmt_fee
mgmt_fee_l1
exp_ratio
exp_ratio_l1
```

The script attaches `turn_ratio`, `mgmt_fee`, and `exp_ratio` from:

```text
crsp_q_mutualfunds.fund_summary2
```

using a portfolio-level as-of join.

Main output:

```text
R Raw Data/mf_with_names_equity_perf_controls_2022_2025.csv
```

This is the main enriched equity-filtered dataset for the 2022-2025 period.

The script then auto-runs:

```text
05_filter_list_for_downloader.R
```

---

### 4.5 `05_filter_list_for_downloader.R`

This script connects the R pipeline to the separate EDGAR downloader and ML workflow.

First, it creates a portfolio-level file for EDGAR searching and downloading:

```text
R Raw Data/TSR Manual Search List - Equity.csv
```

The file contains one representative row per `crsp_portno`.

The selection prioritizes:

1. stronger CIK coverage;
2. more recent observations;
3. website availability;
4. retail-only share classes when useful for searching.

The key columns include:

```text
crsp_portno
crsp_fundno
ticker
fund_name
series_cik
contract_cik
comp_cik
website
city
state
retail_fund
inst_fund
caldt
```

After the downloader and ML classifier are run, the script imports:

```text
Distrbution Dates/tsr_first_tsr_dates_by_fund_ML_FIXED.csv
```

It then attaches the first TSR dates to the full `dt` object by `crsp_portno`.

Additional TSR timing and coverage outputs are saved to:

```text
Descriptive Statistics/
```

These include:

```text
TSR_A_coverage_counts_pct.csv
TSR_B_lag_filing_minus_report_days.csv
TSR_C_adoption_by_filing_month.csv
TSR_D_with_vs_missing_characteristics.csv
```

The script then auto-runs:

```text
06_Create Samples.R
```

---

### 4.6 `06_Create Samples.R`

This script saves the final enriched panel and creates the main event-window sample.

It first saves the full enriched equity-filtered panel:

```text
R Raw Data/Full_equity_filtered_data.csv
```

It then defines the eligible universe as portfolios observed during:

```text
2022-01-01 to 2025-12-31
```

For each eligible portfolio, it creates a fund-specific event window around the first TSR filing month-end:

```text
24 months before TSR adoption
24 months after TSR adoption
```

The main analysis sample is saved as:

```text
R Raw Data/Equity_filtered_24m_pre_post_by_tsr_filingdate_first.csv
```

The script also keeps the event-window sample in memory as:

```text
dt_24
```

This object is used by the descriptive statistics and regression scripts.

---

### 4.7 `07_Analysis.R`

This script creates a missingness diagnostic table for `dt_24`.

It checks key variables by group:

```text
id/core
dependent variables
independent variables
controls
tsr_dates
```

Main outputs:

```text
Descriptive Statistics/missing_table_dt24.csv
Descriptive Statistics/missing_table_dt24.xlsx
```

---

## 5. Descriptive Statistics

The main descriptive-statistics script is:

```text
complete_combined_descriptive_statistics_winsorized.R
```

This script assumes that `dt_24` already exists in memory.

It creates a local analysis copy, so it does **not** overwrite `dt_24`.

The script:

- reconstructs the descriptive helper variables;
- computes Darendeli-style return-adjusted flow for descriptive reporting;
- winsorizes continuous variables at the 1st and 99th percentiles;
- creates a full Excel workbook;
- exports separate CSV and PNG files with clear names;
- adds TSR timing figures and event-window coverage diagnostics.

Main workbook output:

```text
Descriptive Statistics/descriptive_statistics_winsorized_complete_pack.xlsx
```

Workbook sheets include:

```text
01_Core_Descriptives
02_PrePost_Class
03_PrePost_N_Mean_Median
04_Class_Structure
05_TSR_Timing_Summary
06_TSR_Adoption_Monthly
07_TSR_Adoption_Figure
08_Pre_Retail_vs_Inst
09_Manager_Concentration
10_Top10_Managers
11_Quartile_Cutoffs
12_Fee_Quartile_Composition
13_Window_Coverage_Counts
14_Window_Heatmap_Table
15_Window_Heatmap_Figure
16_Window_Coverage_Bar
```

The most important point is that the descriptive tables are produced from a winsorized copy of the analysis sample, not from the raw un-winsorized data.

---

## 6. Main Regression Design

The main regressions study whether performance-flow sensitivity changes after TSR adoption.

The preferred performance measure in the current professor tables is:

```text
alpha_capm_12m
```

The current regression scripts generally use:

```text
Performance variable = alpha_capm_12m at t
Dependent variable   = flow, inflow, or outflow at t+1
```

The post indicator is fund-specific:

```text
post_fund = 1 if caldt >= first TSR filing month-end
```

The main regression structure is:

```text
Flow_{i,t+1} = β1 Performance_{i,t}
             + β2 Post_{p,t}
             + β3 Performance_{i,t} × Post_{p,t}
             + Controls
             + Class fixed effects
             + Year-month fixed effects
             + Error
```

where:

- `i` is the share class;
- `p` is the portfolio;
- class fixed effects are based on `crsp_fundno`;
- year-month fixed effects are based on `ym`;
- standard errors are clustered by `crsp_portno`.

The key coefficient is:

```text
Performance × Post
```

A positive coefficient means that flow-performance sensitivity increased after TSR adoption.

---

## 7. Main Regression Scripts

### 7.1 Net Flow

```text
Main-Darendelis Flows.R
```

This script estimates the main professor table using Darendeli-style net flow at `t+1`.

It produces three displayed model families:

```text
Combined classes
Model 1 - Retail
Model 1 - Institutional
```

The dependent variable is:

```text
Darendeli flow at t+1
```

The performance variable is:

```text
alpha_capm_12m at t
```

The script creates both:

```text
without_winsorization
with_winsorization
```

outputs.

Main result folder:

```text
Results/professor_alpha_capm_12m_dar_tplus1_classFE/
```

---

### 7.2 Inflows

```text
Main- Inflows.R
```

This script estimates the same main structure, but the dependent variable is positive new money only:

```text
inflow at t+1
```

Inflow is created as the positive part of Darendeli flow.

Main result folder:

```text
Results/professor_alpha_capm_12m_inflow_tplus1_classFE/
```

---

### 7.3 Outflows

```text
Main- Outflows.R
```

This script estimates the same main structure, but the dependent variable is withdrawals only:

```text
outflow at t+1
```

Outflow is created as the negative part of Darendeli flow, expressed as a positive withdrawal measure.

Main result folder:

```text
Results/professor_alpha_capm_12m_outflow_tplus1_classFE/
```

---

## 8. Fee-Based Analyses

The project also tests whether TSR effects differ between high-fee and low-fee funds.

The fee grouping is based on average pre-TSR expense ratio. In the high-vs-low scripts, each share class receives a permanent fee group based on its pre-TSR average expense ratio.

---

### 8.1 High-Fee vs Low-Fee Combined Tables

These scripts create one six-column professor table:

```text
High-fee Combined
Low-fee Combined
High-fee Retail
Low-fee Retail
High-fee Institutional
Low-fee Institutional
```

Scripts:

```text
Cross Sec Fee- Darendeli-Combined.R
Cross Sec Fee- Inflows.R
Cross Sec Fee- Outflows.R
```

Main result folders:

```text
Results/alpha_capm12m_dar_flow_tplus1_classFE_highlow_comparison/
Results/alpha_capm12m_inflow_tplus1_classFE_highlow_comparison/
Results/alpha_capm12m_outflow_tplus1_classFE_highlow_comparison/
```

These scripts also add high-vs-low coefficient comparison rows using:

```text
z = (Beta_high - Beta_low) / sqrt(SE_high^2 + SE_low^2)
```

---

### 8.2 Tercile and Quartile Fee Analyses

Additional scripts split funds by fee terciles or quartiles.

Tercile scripts:

```text
Terciles- Darendelis Flows.R
Terciles- Inflows.R
Terciles- Outflows.R
```

Quartile script:

```text
Quartiles- Darendelis Flows.R
```

These scripts keep the same broad structure:

- dependent variable measured at `t+1`;
- performance measured at `t`;
- class and year-month fixed effects;
- clustering by `crsp_portno`;
- t-statistics shown in parentheses;
- outputs created with and without winsorization.

---

## 9. Key Variables

### 9.1 Identifiers

| Variable | Description |
|---|---|
| `crsp_fundno` | Share-class identifier |
| `crsp_portno` | Portfolio identifier |
| `caldt` | Month-end date |
| `ym` | Year-month fixed effect |
| `fund_name` | Fund name |
| `ticker` | Ticker |
| `retail_fund` | CRSP retail flag |
| `inst_fund` | CRSP institutional flag |

### 9.2 TSR Dates

| Variable | Description |
|---|---|
| `tsr_filingdate_first` | First TSR filing date from ML output |
| `tsr_reportdate_first` | First TSR report date from ML output |
| `tsr_filingdate_first_mend` | First TSR filing date converted to month-end |
| `post_fund` | Fund-specific post-TSR indicator |

### 9.3 Flow Variables

| Variable | Description |
|---|---|
| `flow` | Darendeli-style return-adjusted net flow |
| `flow_l1` | Lagged flow |
| `flow_tp1` | Darendeli flow at `t+1` |
| `inflow_tp1` | Positive inflow at `t+1` |
| `outflow_tp1` | Positive withdrawal/outflow at `t+1` |

### 9.4 Performance Variables

```text
alpha_capm_12m
alpha_capm_24m
alpha_capm_36m
alpha_carhart_12m
alpha_carhart_24m
alpha_carhart_36m
fund_ret_12m
fund_ret_60m
fund_ret_120m
excess_ret_12m
excess_ret_60m
excess_ret_120m
```

### 9.5 Controls

```text
age_years_l1
log_tna_l1
flow_l1
log_familytna_l1
turn_ratio_l1
mgmt_fee_l1
exp_ratio_l1
```

Some regression scripts create contemporaneous local controls inside the script, such as:

```text
age_years_t
log_tna_t
flow_dar_t
log_familytna_t
turn_ratio_t
mgmt_fee_t
exp_ratio_t
```

These local variables are used only inside those scripts and do not overwrite the saved pipeline data.

---

## 10. How to Run the Project

### Step 1: Run the Main R Pipeline

From the `Scripts/` folder, run:

```r
source("Data Collection.R")
```

This starts the sequential pipeline:

```text
Data Collection.R
→ 02_equity_filter.R
→ 03_alphas_market_adjusted_returns.R
→ 04_controls_and_save.R
→ 05_filter_list_for_downloader.R
→ 06_Create Samples.R
→ 07_Analysis.R
```

The pipeline pauses after creating the downloader list so that the separate EDGAR downloader and ML classifier can be run.

---

### Step 2: Run the Separate EDGAR Downloader and ML Classifier

Use the following file as the input to the Python downloader:

```text
R Raw Data/TSR Manual Search List - Equity.csv
```

The downloader and ML process should produce:

```text
Distrbution Dates/tsr_first_tsr_dates_by_fund_ML_FIXED.csv
```

After this file is available, continue the R pipeline so the first TSR dates can be attached to `dt`.

---

### Step 3: Create the Final Event-Window Sample

The event-window sample is created by:

```text
06_Create Samples.R
```

Main output:

```text
R Raw Data/Equity_filtered_24m_pre_post_by_tsr_filingdate_first.csv
```

The same object is kept in memory as:

```text
dt_24
```

---

### Step 4: Run Descriptive Statistics

After `dt_24` exists, run:

```r
source("complete_combined_descriptive_statistics_winsorized.R")
```

Main output:

```text
Descriptive Statistics/descriptive_statistics_winsorized_complete_pack.xlsx
```

---

### Step 5: Run Regression Tables

Main net-flow regression:

```r
source("Main-Darendelis Flows.R")
```

Main inflow regression:

```r
source("Main- Inflows.R")
```

Main outflow regression:

```r
source("Main- Outflows.R")
```

High-vs-low fee tables:

```r
source("Cross Sec Fee- Darendeli-Combined.R")
source("Cross Sec Fee- Inflows.R")
source("Cross Sec Fee- Outflows.R")
```

Tercile and quartile fee analyses can then be run if needed.

---

## 11. Output Formats

Most regression scripts export results in multiple formats:

```text
.html
.tex
.xlsx
```

The Excel files are useful for checking formatting and sample sizes. The TeX files are intended for thesis writing or LaTeX tables. The HTML files are useful for quick viewing.

Regression tables report:

- coefficient estimates;
- t-statistics in parentheses;
- sample size;
- R-squared measures;
- fixed effect indicators;
- cluster level;
- dependent variable description.

---

## 12. Packages Used

The project uses the following main R packages:

```r
data.table
dplyr
lubridate
DBI
RPostgres
fixest
modelsummary
openxlsx
kableExtra
ggplot2
scales
```

WRDS access is required for the data extraction scripts.

The separate downloader and ML classifier are run outside the main R pipeline, preferably in Ubuntu VPS.

---

## 13. Data Availability Note

The repository contains code, not the proprietary WRDS/CRSP data.

The following data files are generated locally and should generally not be committed to the public repository:

```text
R Raw Data/*.csv
Distrbution Dates/*.csv
Downloaded EDGAR filings
Rendered PDF filings
Large Excel result workbooks
```

This keeps the repository focused on reproducible code while avoiding redistribution of restricted or very large files.

---

## 14. Current Empirical Focus

The current preferred specification uses:

```text
Dependent variable: Darendeli flow at t+1
Performance: alpha_capm_12m at t
Post indicator: fund-specific first TSR filing month-end
Fixed effects: crsp_fundno + ym
Clustered standard errors: crsp_portno
```

The main interpretation is based on the coefficient:

```text
Perf (t) × Post
```

This coefficient captures whether investor flow-performance sensitivity increases after TSR adoption.

Separate tables examine:

- overall net flows;
- inflows;
- outflows;
- retail share classes;
- institutional share classes;
- combined classes;
- high-fee versus low-fee funds;
- fee terciles;
- fee quartiles.

---

## 15. Practical Note

This repository is a research workflow, not a packaged software library. The scripts are intentionally written in a step-by-step style so that each stage of the data construction and analysis can be inspected, checked, and revised as the thesis develops.
