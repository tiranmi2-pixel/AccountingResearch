# ===================== 02b_tsr_approx_distribution.R =====================
# PURPOSE:
#  - Pull N-CSR / N-CSRS filings from wrdssec.wrds_forms using  dt’s CIKs
#  - Compute approx TSR distribution date per filing using timing logic:
#      rdate+1 <= dist <= min(rdate+60, fdate)
#      and filing no more than ~10 days after dist (assumption)
#  - Keep filings even if timing looks "late" or inconsistent, BUT FLAG them
#  - Pick FIRST TSR-era event per share class (crsp_fundno)
#  - Attach to monthly panel dt (append cols; no reorder)

library(data.table)
library(DBI)
library(lubridate)

stopifnot(exists("dt"), exists("wrds"), DBI::dbIsValid(wrds))
setDT(dt)
old_cols <- names(dt)

attach_empty <- function() {
  # create empty columns if missing so pipeline doesn’t break
  date_cols <- c("approx_tsr_dis","approx_tsr_dis_monthend","tsr_fdate_first","tsr_rdate_first","tsr_rdate_raw_first")
  char_cols <- c("tsr_form_first","tsr_accession_first","tsr_cik_used","tsr_cik_type_used")
  flag_cols <- c("tsr_flag_pre_tsr_period","tsr_flag_inconsistent_window","tsr_flag_filed_too_late_for_60")
  for (v in date_cols) if (!(v %in% names(dt))) dt[, (v) := as.Date(NA)]
  for (v in char_cols) if (!(v %in% names(dt))) dt[, (v) := NA_character_]
  for (v in flag_cols) if (!(v %in% names(dt))) dt[, (v) := as.integer(NA)]
  if (!("post_tsr" %in% names(dt))) dt[, post_tsr := 0L]
  new_cols <- setdiff(names(dt), old_cols)
  setcolorder(dt, c(old_cols, new_cols))
  assign("dt", dt, envir = .GlobalEnv)
}

# -----------------------
# Settings
# -----------------------
CUTOFF <- as.Date("2024-07-31")           # TSR cutoff you are using
DIST_MAX_DAYS <- 60L                      # max allowed time after rdate to distribute TSR (assumption)
LAG_DAYS_ASSUMED <- 10L                   # filing happens up to ~10 days after distribution (assumption)

FORMS <- c("N-CSR","N-CSRS","N-CSR/A","N-CSRS/A","NT-NCSR","NTFNCSR")

FDATE_MIN <- as.Date("2020-01-01")
FDATE_MAX <- as.Date(max(dt$caldt, na.rm = TRUE) + 365)  # generous upper bound

# If TRUE, we only consider filings whose calculated distribution is after cutoff
KEEP_ONLY_POST_DIST <- TRUE

# -----------------------
# 0) Build fundno -> filing-CIK mapping  (COMP_CIK ONLY; removes unused series/contract work)
# -----------------------
pad10 <- function(x) {
  x <- trimws(as.character(x))
  x[x == ""] <- NA_character_
  x <- sub("\\.0+$", "", x)
  out <- ifelse(!is.na(x) & grepl("^[0-9]+$", x), sprintf("%010d", as.integer(x)), x)
  out
}

if (!("comp_cik" %in% names(dt))) {
  cat("[02b] dt has no comp_cik column; attaching NA columns only.\n")
  attach_empty()
  quit(save="no")
}

# base map from dt (keep same structure you had, but we only use comp_cik)
cik_map <- unique(dt[, .(crsp_fundno, comp_cik)])
cik_map[, comp_cik := pad10(comp_cik)]

# comp mapping only
comp_long <- cik_map[!is.na(comp_cik) & grepl("^[0-9]{10}$", comp_cik),
                     .(crsp_fundno, cik = comp_cik, cik_type="comp_cik", pri=1L)]

# dedupe
cik_long <- unique(comp_long, by=c("crsp_fundno","cik"))

# IMPORTANT: only numeric CIKs go into wrds_forms query
cik_long[, cik := pad10(cik)]
cik_long <- cik_long[grepl("^[0-9]{10}$", cik)]
all_ciks <- unique(cik_long$cik)

cat("[02b] Using ONLY comp_cik.\n")
cat("[02b] Share classes with valid comp_cik:", uniqueN(cik_long$crsp_fundno),
    "out of", uniqueN(dt$crsp_fundno), "\n")
cat("[02b] Numeric 10-digit CIKs usable for wrdssec.wrds_forms:", length(all_ciks), "\n")

# ✅ guard: avoid crash in pull_wrds_forms
if (length(all_ciks) == 0) {
  cat("[02b] No usable comp_cik values; attaching NA columns only.\n")
  attach_empty()
  quit(save="no")
}

# -----------------------
# 1) Helper:  pull from wrdssec.wrds_forms
# -----------------------
pull_wrds_forms <- function(ciks, chunk_size = 800L) {
  stopifnot(length(ciks) > 0)
  out_list <- vector("list", ceiling(length(ciks) / chunk_size))
  k <- 1L
  
  for (i in seq(1L, length(ciks), by = chunk_size)) {
    csub <- ciks[i:min(i + chunk_size - 1L, length(ciks))]
    in_clause <- paste0("('", paste(csub, collapse = "','"), "')")
    
    q <- sprintf("
      SELECT
        cik, coname, form, fdate, rdate, secadate, secpdate, accession
      FROM wrdssec.wrds_forms
      WHERE cik IN %s
        AND fdate BETWEEN '%s' AND '%s'
        AND UPPER(form) IN ('%s')
    ", in_clause,
                 format(FDATE_MIN, "%Y-%m-%d"),
                 format(FDATE_MAX, "%Y-%m-%d"),
                 paste(toupper(FORMS), collapse="','"))
    
    tmp <- as.data.table(DBI::dbGetQuery(wrds, q))
    if (nrow(tmp) > 0) {
      tmp[, `:=`(
        fdate = as.Date(fdate),
        rdate = as.Date(rdate),
        secadate = as.Date(secadate),
        secpdate = as.Date(secpdate)
      )]
    }
    out_list[[k]] <- tmp
    cat(sprintf("[02b] Pulled chunk %d/%d: %d rows\n", k, length(out_list), nrow(tmp)))
    k <- k + 1L
  }
  rbindlist(out_list, use.names = TRUE, fill = TRUE)
}

forms_dt <- pull_wrds_forms(all_ciks)
forms_dt[, cik := pad10(cik)]
cat("[02b] Total matched filing rows:", nrow(forms_dt), "\n")

if (nrow(forms_dt) == 0) {
  cat("[02b] No N-CSR/N-CSRS filings matched your comp_cik list; attaching NA columns only.\n")
  attach_empty()
  quit(save="no")
}

# -----------------------
# 2) Compute approx distribution date per filing + flags

# -----------------------
forms_dt[, `:=`(
  fdate    = as.Date(fdate),
  rdate    = as.Date(rdate),
  secadate = as.Date(secadate),
  secpdate = as.Date(secpdate)
)]

# Some WRDS/SEC rows have missing rdate for funds.
# Use secpdate as fallback "period end" when rdate is missing.
forms_dt[, rdate_use := fifelse(!is.na(rdate), rdate, secpdate)]

# ✅ IMPORTANT: use !is.na() (NOT is.finite()) for Dates
forms_dt <- forms_dt[!is.na(fdate) & !is.na(rdate_use)]

# Key dates:
forms_dt[, dist_lb := rdate_use + 1L]
forms_dt[, dist_ub_by_filing := fdate]
forms_dt[, dist_ub_60 := rdate_use + DIST_MAX_DAYS]
forms_dt[, dist_ub := pmin(dist_ub_by_filing, dist_ub_60, na.rm = TRUE)]

# Feasible window
forms_dt[, dist_lb_feasible := pmax(dist_lb, fdate - LAG_DAYS_ASSUMED, na.rm = TRUE)]
forms_dt[, dist_ub_feasible := dist_ub]

# Flags
forms_dt[, tsr_flag_pre_tsr_period := as.integer(rdate_use <= CUTOFF)]
forms_dt[, tsr_flag_inconsistent_window := as.integer(dist_lb_feasible > dist_ub_feasible)]
forms_dt[, tsr_flag_filed_too_late_for_60 := as.integer(fdate > (rdate_use + DIST_MAX_DAYS + LAG_DAYS_ASSUMED))]

# Choose approx distribution date
forms_dt[, approx_tsr_dis := as.Date(NA)]

forms_dt[tsr_flag_inconsistent_window == 0L,
         approx_tsr_dis := dist_ub_feasible]

forms_dt[tsr_flag_inconsistent_window == 1L,
         approx_tsr_dis := pmax(dist_lb, as.Date(fdate - LAG_DAYS_ASSUMED), na.rm = TRUE)]

forms_dt <- forms_dt[!is.na(approx_tsr_dis)]

# Optional: keep only post-cutoff inferred distribution
if (KEEP_ONLY_POST_DIST) {
  forms_dt <- forms_dt[approx_tsr_dis > CUTOFF]
}

# -----------------------
# 3) Map filings back to share classes via cik_long
# -----------------------
setkey(cik_long, cik)
setkey(forms_dt, cik)

hit <- forms_dt[cik_long, on = .(cik), allow.cartesian = TRUE]

if (nrow(hit) == 0) {
  cat("[02b] No filings matched to fund share classes; attaching NA columns only.\n")
  attach_empty()
  quit(save="no")
}

# -----------------------
# 4) Choose FIRST TSR-era event per share class (crsp_fundno)
# -----------------------
hit[, amend_flag := as.integer(grepl("/A$", toupper(form)))]

setorder(hit,
         crsp_fundno,
         approx_tsr_dis,
         pri,
         fdate,
         amend_flag)

first_event <- hit[, .SD[1], by = crsp_fundno]

first_event[, approx_tsr_dis_monthend := (as.Date(ceiling_date(approx_tsr_dis, "month")) - 1L)]

first_event <- first_event[, .(
  crsp_fundno,
  approx_tsr_dis,
  approx_tsr_dis_monthend,
  tsr_form_first = form,
  tsr_fdate_first = fdate,
  tsr_rdate_first = rdate_use,          # ✅ use effective rdate
  tsr_rdate_raw_first = rdate,          # (optional)
  tsr_accession_first = accession,
  tsr_cik_used = cik,
  tsr_cik_type_used = cik_type,
  tsr_flag_pre_tsr_period,
  tsr_flag_inconsistent_window,
  tsr_flag_filed_too_late_for_60
)]

# -----------------------
# 5) Attach to dt (update-join; append cols only)
# -----------------------
dt[, crsp_fundno := as.numeric(crsp_fundno)]
first_event[, crsp_fundno := as.numeric(crsp_fundno)]

setkey(dt, crsp_fundno, caldt)
setkey(first_event, crsp_fundno)

dt[first_event, `:=`(
  approx_tsr_dis = i.approx_tsr_dis,
  approx_tsr_dis_monthend = i.approx_tsr_dis_monthend,
  tsr_form_first = i.tsr_form_first,
  tsr_fdate_first = i.tsr_fdate_first,
  tsr_rdate_first = i.tsr_rdate_first,
  tsr_rdate_raw_first = i.tsr_rdate_raw_first,
  tsr_accession_first = i.tsr_accession_first,
  tsr_cik_used = i.tsr_cik_used,
  tsr_cik_type_used = i.tsr_cik_type_used,
  tsr_flag_pre_tsr_period = i.tsr_flag_pre_tsr_period,
  tsr_flag_inconsistent_window = i.tsr_flag_inconsistent_window,
  tsr_flag_filed_too_late_for_60 = i.tsr_flag_filed_too_late_for_60
)]

if (!("post_tsr" %in% names(dt))) dt[, post_tsr := 0L]
dt[, post_tsr := as.integer(!is.na(approx_tsr_dis_monthend) & caldt >= approx_tsr_dis_monthend)]

# Order preservation
new_cols <- setdiff(names(dt), old_cols)
setcolorder(dt, c(old_cols, new_cols))

cat("[02b] Share classes with event date:", uniqueN(dt[!is.na(approx_tsr_dis), crsp_fundno]),
    "out of", uniqueN(dt$crsp_fundno), "\n")

cat("[02b] Match type breakdown (share-class count):\n")
print(unique(dt[!is.na(tsr_cik_type_used), .(crsp_fundno, tsr_cik_type_used)])[
  , .N, by = tsr_cik_type_used
][order(-N)])

assign("dt", dt, envir = .GlobalEnv)
