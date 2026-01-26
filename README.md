# AccountingResearch
The repository contains the R code for the TSR research.


# TSR Data Collection (CRSP Mutual Funds + WRDS SEC Forms)

## Part 01: Project objectives and overall research approach

### 1.1 Project background

This repository contains an end-to-end R workflow that constructs a monthly CRSP mutual-fund share-class panel, combine it with fund identifiers and contact information, filters to U.S. domestic equity mutual funds, and then attaches TSR-distrbution date (Estimated value) from SEC filing metadata. The workflow also computes performance measures such as CAPM/Carhart rolling alphas &t martket adjusted trailing returns and control variables required for the study (e.g., lagged size, age, fees, turnover, volatility, loads).


### 1.2 Overall research approach (how the pipeline works)

The workflow is designed as a sequential build → filter → combine → save process, where each script adds a specific layer of information while preserving column order (identifiers left; newly created variables appended right). The scripts share a common convention: the master dataset is held in memory as **`dt`** (data.table).

**High level flow of the scripts**

1. Build base monthly panel (`dt`) from CRSP and attach mapping/contact/style(investment type, eg : Whether its equity or bond fund)/CIKs
2. Filter to domestic equity mutual funds (exclude ETFs/ETNs, exclude index funds, keep CRSP ED objective; validate with Lipper EQ when available)
3. Attach TSR event proxy from WRDS SEC forms and create `post_tsr` indicator - Event proxy is the first month end date when a fund is inferred to have distributed the TSR report after the mandatory cut of July 2024.
4. Compute alphas, trailing returns, and market-adjusted performance
5. Add controls and save the final CSV
6. Create a manual TSR search list for EDGAR/website validation - This file is later used with google colabotory python script to download all NCSR filings after January 1st 2021. The downloader downloads PDF rendered version of the filing and HTML version which can be later used for xbrl related analysis if required. 
7. Create analysis samples (event window and fixed calendar window) _ Filtered data collection is saved as two seperate CVs, one contains the details of 24 months pre and post the first TSR distrubiton date of each fund. Second file consists of data sample covering 48 months from 2022 January to December 2025.

---

## Part 02: Code map (what each R file does)

### 2.1 `01_data_extraction.R` (a.k.a. “Data Collection.R”) — Base dataset construction

**Purpose**

* Connects to WRDS with an **auto-reconnect `dbGetQuery()` wrapper** to reduce failures in long runs.
* Pulls CRSP monthly fund data (`monthly_tna_ret_nav`) and converts returns to decimals if needed.
* Pulls Fama-French monthly factors and builds a market index series (`mkt_idx_df`).
* Joins CRSP `portnomap` and `contact_info` using their valid date windows.
* Pulls style (investment type) fields from `fund_summary2` (e.g., `lipper_asset_cd`, `policy`) using a rolling “as-of” join.
* Attaches CIK fields (`comp_cik`, `series_cik`, `contract_cik`) via `crsp_cik_map` and creates `cik_best`.
* Saves key objects into memory (`dt`, `ff`, `mkt_idx_df`, `proj_root`, `wrds`) and **auto-runs Script 02**. 

**Data stored in memory**

* `dt`: base monthly panel
* `ff`: monthly FF factors (decimal)
* `mkt_idx_df`: market return series by month (`mkt_ret = mktrf + rf`)
* `proj_root`, `wrds` 

---

### 2.2 `02_equity_filter.R` — Equity mutual fund filtering + style attach (as-of)

**Purpose**

* Selects the crsp_q_mutualfunds.fund_summary2 and joins `crsp_obj_cd`, `policy`, and `lipper_asset_cd` using an as-of rolling join.
* Then applies a structured equity filter:

  1. Drop ETFs/ETNs using `et_flag` (F/N)
  2. Drop index funds using `index_fund_flag` (B/D/E)
  3. Keep domestic equity using CRSP objective code `ED**`
  4. Validate with Lipper: keep `EQ` when present; if missing, keep CRSP classification 

**Additional outputs created by this script**

* An **info-only** filter diagnostic file for 2021–2025:

  * `filter_drop_summary_2021_2025_with_lipper_match.csv`
    This tracks how many unique funds are removed at each filter step and how often Lipper confirms/mismatches CRSP equity classification. 

**Unfiltered “snapshot” (important)**

*The script also creates an unfiltered snapshot of the data (dt_unfilt). It then runs the same enrichment steps (02b → 03 → 04) on this snapshot and saves:

*mf_unfiltered_all_final_columns.csv

*This file contains the full CRSP mutual-fund universe (i.e., no equity/ETF/index filtering), but it still includes all derived variables—TSR event fields, rolling alphas, trailing returns, and control variables—so it matches the structure of the main analysis dataset.

**Finalizing**

* After filtering the exisitng data using the criterias explained above, it auto-runs the below two scripts to obtain estimated TSR distribution date and derive alphas and market adjusted returns:

  * `02b_tsr_approx_distribution.R`
  * `03_alphas_market_adjusted_returns.R` 

---

### 2.3 `02b_tsr_approx_distribution.R` — TSR event proxy from WRDS SEC forms
*  As no source is available to reliably extract TSR distribution date, we will use a proxy which is calculated based on available data in wrds and using distribution rules imposed by sec for reporting time period.
* **Step 1 — Pull TSR-relevant SEC filings using fund CIKs (COMP CIK only).**
  For each share class (`crsp_fundno`), the script takes `comp_cik`, convert it to a clean 10-digit numeric CIK, and queries `wrdssec.wrds_forms` for TSR-relevant forms:
  `N-CSR`, `N-CSRS`, amendments (`/A`), and notices (`NT-NCSR`, `NTFNCSR`).

* **Step 2 — Define the filing dates used in the timing logic.**
  For each matched filing row:

  * `fdate` = SEC filing date (EDGAR filing date)
  * `rdate_use` = reporting period end date used for timing

    * if `rdate` is missing, the script falls back to `secpdate` as the period-end proxy.

* **Step 3 — Construct a feasible window for “distribution date” (because true distribution is unobserved).**
  The script builds an approximate distribution window using two assumptions:

  **(A) Distribution must occur after the report period ends**

  * Lower bound:
    `dist_lb = rdate_use + 1`

  **(B) Distribution must occur no later than the earlier of filing date and 60 days after period end**

  * Upper bound:
    `dist_ub = min(fdate, rdate_use + 60)`

  **(C) “Filed within 10 days of distribution” assumption (tightens the lower bound)**

  * Lower bound:
    `dist_lb_feasible = max(dist_lb, fdate - 10)`
  * Upper bound stays:
    `dist_ub_feasible = dist_ub`

* **Step 4 — Flag questionable timing (but keep filings).**
  The script keeps filings even if assumptions conflict, but flags them:

  * `tsr_flag_pre_tsr_period = 1` if `rdate_use <= 2024-07-31`
  * `tsr_flag_inconsistent_window = 1` if `dist_lb_feasible > dist_ub_feasible`
  * `tsr_flag_filed_too_late_for_60 = 1` if `fdate > rdate_use + 60 + 10` Eg: Late filing

* **Step 5 — Choose the approximate distribution date per filing (`approx_tsr_dis`).**
  This is the key event proxy calculation:

  **If the feasible window is consistent** (`dist_lb_feasible ≤ dist_ub_feasible`):

  * the script sets
    `approx_tsr_dis = dist_ub_feasible`
     meaning pick the latest feasible date** (usually the filing date unless it exceeds rdate+60).

  **If the feasible window is inconsistent**:

  * the script falls back to
    `approx_tsr_dis = max(rdate_use + 1, fdate - 10)`
   Meaning: best-effort estimate that respects “after period end” and “~10 days before filing”.

  Then, because `KEEP_ONLY_POST_DIST = TRUE`, it drops filings where:

  * `approx_tsr_dis <= 2024-07-31`

* **Step 6 — Pick the “first TSR-era event” per share class.**
  After mapping filings back from `cik → crsp_fundno`, the script sorts and selects one filing per `crsp_fundno`:

  Sorting priority:

  1. **earliest `approx_tsr_dis`** (defines the first event)
  2. earlier `fdate`
  3. prefer non-amendments over amendments (tie-break)

  The first row after sorting becomes the share class’s TSR event.

* **Step 7 — Convert to month-end and create `post_tsr`.**
  Because CRSP panel date `caldt` is **monthly (month-end)**, the event date is converted to month-end:

  * `approx_tsr_dis_monthend = ceiling_date(approx_tsr_dis, "month") - 1`

  Then the post indicator is defined as:

  * `post_tsr = 1` if `caldt >= approx_tsr_dis_monthend` (and the event exists), else `0`

  Interpretation:

  * Months before the event month-end are **pre-TSR**
  * The event month-end and later months are **post-TSR**

  * `approx_tsr_dis_monthend`
  * `post_tsr` = 1 once `caldt >= approx_tsr_dis_monthend` 

**Key columns appended to `dt`**

* `approx_tsr_dis`, `approx_tsr_dis_monthend`
* `tsr_form_first`, `tsr_accession_first`
* `tsr_fdate_first`, `tsr_rdate_first`, `tsr_rdate_raw_first`
* `tsr_cik_used`, `tsr_cik_type_used`
* Quality flags: `tsr_flag_pre_tsr_period`, `tsr_flag_inconsistent_window`, `tsr_flag_filed_too_late_for_60`
* `post_tsr` 

---

### 2.4 `03_alphas_market_adjusted_returns.R` — Performance construction

### Purpose 

This script combines0 the monthly CRSP share-class panel `dt` with factor data and performance measures, without changing the existing column order  (it only appends new variables to the far right), then hands off to Script 04.

* **Merge Fama–French factors by month**

  * Joins the monthly FF table onto `dt` using the month-end date `caldt` (i.e., each fund-month row gets the same factor values for that calendar month).

* **Build excess return**

  * Creates `exret = mret − rf` (fund’s monthly return minus the risk-free rate for that month).

* **Compute fund flows (if missing)**

  * If not already present, computes:

    * `flow`: CRSP-style net flow measure using current TNA, lagged TNA, and return.
    * `flow_l1`: one-month lag of `flow` (within `crsp_fundno`).

* **Estimate rolling alphas via rolling regressions**

  * Runs rolling  time-series regressions within each share class (`crsp_fundno`) using `frollapply` + `lm.fit`.
  * Outputs  rolling intercepts (alphas) for:

    * **CAPM alpha  (regress `exret` on `mktrf`) over 12 / 24 / 36 months
    * **Carhart alpha** (regress `exret` on `mktrf, smb, hml, umd`) over 12 / 24 / 36  months

* **Compute trailing compounded returns (long-horizon performance)**

  * Builds trailing compounded returns using rolling compounding for:

    * **Market**: `mkt_ret_12m`, `mkt_ret_60m`, `mkt_ret_120m`
    * **Fund**: `fund_ret_12m`, `fund_ret_60m`, `fund_ret_120m`

* **Compute market-adjusted trailing returns**

  * Creates market-adjusted (“excess vs market”) trailing performance:

    * `excess_ret_12m  = fund_ret_12m  − mkt_ret_12m`
    * `excess_ret_60m  = fund_ret_60m  − mkt_ret_60m`
    * `excess_ret_120m = fund_ret_120m − mkt_ret_120m`

* **Preserve column order**

  * Keeps all original columns in the same positions; any new factor/flow/alpha/return fields are appended at the end.

* **Auto-run Script 04**

  * After building these measures, the script automatically runs `04_controls_and_save.R` to add controls and write the final CSV. 


### 2.5 `04_controls_and_save.R` — Controls + final dataset export

**Purpose**

* Adds standard regression controls (constructed from the panel and from CRSP fund summary tables), while preserving existing column order:

  * Age (and lag): `age_years`, `age_years_l1`
  * Size (and lag): `log_tna`, `log_tna_l1`
  * Flows (and lag): `flow`, `flow_l1` (if missing)
  * Family size (lagged): `family_tna_l1`, `log_familytna_l1` by `mgmt_cd × caldt`
  * Volatility: `vol_12m`, `vol_12m_l1`
  * Fees/turnover (as-of from `crsp.fund_summary2` or `crsp.fund_summary`): `exp_ratio`, `turn_ratio` and lags
  * Optional loads (if tables exist in `crspq`): `front_load`, `rear_load` and lags 

**Final output produced here**

* `mf_with_names_2015_2026_equity_perf_controls.csv`
  This is the main “analysis-ready” file containing identifiers, TSR event fields, performance metrics, and controls. 

---

### 2.6 `05_filter_equity_funds.R` — TSR manual search list (for validation + downloader input)

**Purpose**

* Builds a compact CSV used for downloading the NCSR filings from the EDGAR:

  * manual website searches (TSR presence)
  * EDGAR downloader workflows (CIK-driven)
* Expects `dt_analysis` to exist in memory and selects a subset of columns (IDs, CIKs, website fields, retail/inst flags).
* Chooses **one representative row per portfolio** (`crsp_portno`) with priority:

  1. has website
  2. retail-only share class
  3. most recent `caldt` 

**Output**

* `TSR Manual Search List - Equity.csv` 



---

### 2.7 `06_Create Samples.R` — Analysis sample construction (two alternative windows)

**Purpose**
Creates two separate CSVs from the final dataset `mf_with_names_2015_2026_equity_perf_controls.csv`:

**(A) Event-window dataset (fund-specific TSR timing)**

* Uses a TSR event date column (first found among):

  * `approx_tsr_dis_monthend` (preferred)
  * `approx_tsr_dis` (converted to month-end)
  * fallback names if renamed
* Keeps observations within **±24 months** around the TSR event month-end. 

**(B) Calendar-window dataset (fixed time window)**

* Keeps observations between **2022-01-01 and 2025-12-31** (inclusive), based on the monthly panel date `caldt`. 

**Outputs**

* `mf_with_names_2015_2026_equity_perf_controls_EVENTWIN_PRE24_POST24_byTSR.csv`
* `mf_with_names_2015_2026_equity_perf_controls_CALWIN_2022_2025_end2025-12-31.csv` 

---

## Part 03: Project file map (recommended folder tree)



```text
AccountingResearch/
├─ Scripts/
│  ├─ 01_Data Collection.R                 # (may also be named "Data Collection.R")
│  ├─ 02_equity_filter.R
│  ├─ 02b_tsr_approx_distribution.R
│  ├─ 03_alphas_market_adjusted_returns.R
│  ├─ 04_controls_and_save.R
│  ├─ 05_filter_equity_funds.R
│  └─ 06_Create Samples.R
│
├─ R Raw Data/   
│  ├─ mf_with_names_2015_2026_equity_perf_controls.csv
│  ├─ mf_unfiltered_all_final_columns.csv
│  ├─ filter_drop_summary_2021_2025_with_lipper_match.csv
│  ├─ TSR Manual Search List - Equity.csv
│  ├─ mf_with_names_2015_2026_equity_perf_controls_EVENTWIN_PRE24_POST24_byTSR.csv
│  └─ mf_with_names_2015_2026_equity_perf_controls_CALWIN_2022_2025_end2025-12-31.csv
│
└─ (optional) TSR reports/     

- [Folder link to download outputs from the R code](https://drive.google.com/drive/folders/1yWs9PBoXDkqW1r-EQgwt7rFwvCFhRva3?usp=sharing)
- [Folder link to view downloaded NCSR reports](https://drive.google.com/drive/folders/1h5ojtapR1Is6RI-xLVu1_4eboPaZOPT_)



---

## Part 04: Output files and what they represent

### 4.1 Main dataset outputs

1. **`mf_with_names_2015_2026_equity_perf_controls.csv`**

   * The primary dataset: equity-filtered CRSP mutual funds with TSR event fields, performance measures, and controls. 

2. **`mf_unfiltered_all_final_columns.csv`**

   * A “full universe” version (no equity/ETF/index filters), but enriched with the same TSR/performance/control columns by temporarily running 02b/03/04 on an unfiltered snapshot.

### 4.2 Diagnostics and validation outputs

3. **`filter_drop_summary_2021_2025_with_lipper_match.csv`**

   * A stepwise breakdown showing how many funds are removed by each filter rule, plus a Lipper-vs-CRSP equity agreement check (computed for interpretability, not for altering the main dataset directly). 

4. **`TSR Manual Search List - Equity.csv`**

   * A portfolio-level list (one row per `crsp_portno`) designed for manual TSR searching and EDGAR downloader inputs, prioritized toward share classes with websites and retail availability. 

### 4.3 Analysis sample outputs (two alternative designs)

5. **`...EVENTWIN_PRE24_POST24_byTSR.csv`**

   * A fund-specific event window sample using the TSR month-end proxy (preferred: `approx_tsr_dis_monthend`) and keeping ±24 months around the event. 

6. **`...CALWIN_2022_2025_end2025-12-31.csv`**

   * A fixed calendar window sample (2022–2025 inclusive), filtered purely by `caldt`. 

---

## Part 05: How to run (recommended sequence)

1. Open RStudio with working directory at the repo root.
2. Run:

   * `Scripts/01_data_extraction.R`
     This will auto-run 02 → 02b → 03 → 04 and produce the main CSV.
3. (Optional) Create `dt_analysis` (your chosen analysis subset) and run:

   * `Scripts/05_filter_equity_funds.R` to generate the manual search list. 
4. Run:

   * `Scripts/06_Create Samples.R` to generate the two analysis CSVs. 

---


