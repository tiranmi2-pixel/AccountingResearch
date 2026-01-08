# ===================== TSR DOWNLOAD AUTOMATION (SEC EDGAR) =====================

rm(list = intersect(c("evaluated_keys","evaluated_key_set","log_detail","existing_fund_log"), ls()),
   envir = .GlobalEnv)

# -----------------------
# 0) Packages
# -----------------------
pkgs <- c("data.table", "jsonlite", "httr2", "stringi")
to_install <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(to_install)) install.packages(to_install)

library(data.table)
library(jsonlite)
library(httr2)
library(stringi)

# -----------------------
# 0B) Input: TSR list CSV
# -----------------------
tsr_csv <- "C:/Users/tiran/OneDrive/Desktop/Research Github/AccountingResearch/R Raw Data/TSR Manual Search List - Equity.csv"

if (!file.exists(tsr_csv)) {
  stop("TSR list CSV not found at: ", tsr_csv)
}

tsr_dt <- fread(tsr_csv, encoding = "UTF-8", na.strings = c("", "NA", "N/A"))
if (nrow(tsr_dt) == 0) stop("TSR list CSV loaded but has 0 rows: ", tsr_csv)

setnames(tsr_dt, tolower(names(tsr_dt)))

required_cols <- c("crsp_portno", "fund_name", "ticker")
missing_cols <- setdiff(required_cols, names(tsr_dt))
if (length(missing_cols)) {
  stop(
    "TSR CSV is missing required columns: ",
    paste(missing_cols, collapse = ", "),
    "\nColumns present: ",
    paste(names(tsr_dt), collapse = ", ")
  )
}

tsr_dt[, ticker := toupper(trimws(as.character(ticker)))]
tsr_dt[, fund_name := as.character(fund_name)]
tsr_dt[, crsp_portno := as.numeric(crsp_portno)]

# -----------------------
# 1) Output folders + logs
# -----------------------
out_root <- file.path(getwd(), "TSR Downloads")
out_raw  <- file.path(out_root, "raw_files")
dir.create(out_raw, recursive = TRUE, showWarnings = FALSE)

if (!dir.exists(out_root) || !dir.exists(out_raw)) {
  stop("Could not create output folders. Check permissions for: ", normalizePath(getwd(), winslash = "/"))
}

log_file_fund   <- file.path(out_root, "tsr_download_log.csv")
log_file_detail <- file.path(out_root, "tsr_download_log_detail.csv")
log_file_move   <- file.path(out_root, "tsr_move_log.csv")

message("Working directory: ", normalizePath(getwd(), winslash = "/"))
message("Output root:      ", normalizePath(out_root, winslash = "/"))
message("Output raw_files: ", normalizePath(out_raw,  winslash = "/"))

# -----------------------
# 1B) Pause/Stop
# -----------------------
pause_file <- file.path(out_root, "PAUSE")
stop_file  <- file.path(out_root, "STOP")

check_pause_stop <- function() {
  if (file.exists(stop_file)) return("STOP")
  if (file.exists(pause_file)) {
    message("PAUSED (remove ./TSR Downloads/PAUSE to continue).")
    while (file.exists(pause_file)) {
      if (file.exists(stop_file)) return("STOP")
      Sys.sleep(2)
    }
    message("RESUMING...")
  }
  "OK"
}

# -----------------------
# 1C) Move CIK folders to Google Drive
# -----------------------
gdrive_root <- "I:/My Drive/Data Collection"
dir.create(gdrive_root, recursive = TRUE, showWarnings = FALSE)

safe_move_dir <- function(src_dir, dst_dir) {
  dir.create(dirname(dst_dir), recursive = TRUE, showWarnings = FALSE)
  
  ok <- FALSE
  try({ ok <- file.rename(src_dir, dst_dir) }, silent = TRUE)
  if (isTRUE(ok)) return(TRUE)
  
  dir.create(dst_dir, recursive = TRUE, showWarnings = FALSE)
  
  src_files <- list.files(
    src_dir, full.names = TRUE, recursive = TRUE, all.files = TRUE, no.. = TRUE
  )
  if (length(src_files) > 0) {
    for (f in src_files) {
      rel <- substring(f, nchar(src_dir) + 2L)  # +2 for separator
      dest_path <- file.path(dst_dir, rel)
      dir.create(dirname(dest_path), recursive = TRUE, showWarnings = FALSE)
      file.copy(f, dest_path, overwrite = TRUE, recursive = FALSE)
    }
  }
  
  unlink(src_dir, recursive = TRUE, force = TRUE)
  TRUE
}

maybe_move_cik_batch <- function(out_raw, gdrive_root, exclude_cik10 = NULL, batch_n = 25L) {
  if (!dir.exists(gdrive_root)) {
    warning("Google Drive path not found/created: ", gdrive_root)
    return(invisible(FALSE))
  }
  
  cik_dirs <- list.dirs(out_raw, full.names = TRUE, recursive = FALSE)
  cik_dirs <- cik_dirs[grepl("CIK[0-9]{10}$", basename(cik_dirs))]
  
  if (!is.null(exclude_cik10) && nzchar(exclude_cik10)) {
    cik_dirs <- cik_dirs[basename(cik_dirs) != paste0("CIK", exclude_cik10)]
  }
  
  if (length(cik_dirs) < batch_n) return(invisible(FALSE))
  
  fi  <- file.info(cik_dirs)
  ord <- order(fi$mtime, na.last = TRUE)
  pick <- cik_dirs[ord][1:batch_n]
  
  move_log <- data.table()
  for (src in pick) {
    dst <- file.path(gdrive_root, basename(src))
    err <- ""
    ok <- tryCatch(safe_move_dir(src, dst), error = function(e) { err <<- as.character(e); FALSE })
    
    move_log <- rbind(
      move_log,
      data.table(
        moved_at = as.character(Sys.time()),
        src = src,
        dst = dst,
        ok = as.integer(isTRUE(ok)),
        error = err
      ),
      fill = TRUE
    )
  }
  
  if (nrow(move_log)) {
    if (file.exists(log_file_move)) {
      old <- tryCatch(fread(log_file_move), error = function(e) data.table())
      move_log <- rbind(old, move_log, fill = TRUE)
    }
    fwrite(move_log, log_file_move, bom = TRUE)
    message("Moved ", sum(move_log$ok == 1L), "/", nrow(move_log), " CIK folders to Google Drive.")
  }
  
  invisible(TRUE)
}

# -----------------------
# 1D) Fresh start toggle
# -----------------------
fresh_start <- TRUE   # TRUE = start clean; FALSE = resume
if (isTRUE(fresh_start)) skip_previously_matched <- FALSE

if (isTRUE(fresh_start)) {
  message("FRESH START: deleting prior logs and starting clean.")
  
  for (f in c(log_file_fund, log_file_detail, log_file_move)) {
    if (file.exists(f)) {
      ok <- file.remove(f)
      if (!isTRUE(ok)) {
        stop(
          "Could not delete log file (it may be OPEN/LOCKED). Close Excel/RStudio Viewer and retry: ",
          normalizePath(f, winslash = "/")
        )
      }
    }
  }
  
  log_detail <- data.table()
  existing_fund_log <- data.table()
  evaluated_keys <- data.table()
}

message("DETAIL LOG EXISTS? ", file.exists(log_file_detail))
message("FUND LOG EXISTS?   ", file.exists(log_file_fund))

# -----------------------
# 2) SEC headers + request helpers
# -----------------------
sec_user_agent <- "Tiran (tiranm11@outlook.com)"
sec_delay_s    <- 0.30

sec_perform_with_retry <- function(req, max_tries = 5L, base_sleep = 1.0) {
  last_err <- NULL
  
  for (try_i in seq_len(max_tries)) {
    Sys.sleep(sec_delay_s)
    
    out <- tryCatch(
      req_perform(req),
      error = function(e) {
        last_err <<- e
        NULL
      }
    )
    
    if (!is.null(out)) {
      st <- resp_status(out)
      if (st < 400) return(out)
      
      if (st %in% c(408, 429, 500, 502, 503, 504)) {
        wait <- base_sleep * (1.7 ^ (try_i - 1))
        message("SEC transient HTTP ", st, " (try ", try_i, "/", max_tries, "). Sleeping ", round(wait, 2), "s...")
        Sys.sleep(wait)
        next
      }
      
      stop("HTTP ", st, " for request: ", resp_url(out))
    }
    
    wait <- base_sleep * (1.7 ^ (try_i - 1))
    message(
      "SEC request error (try ", try_i, "/", max_tries, "): ", conditionMessage(last_err),
      " | Sleeping ", round(wait, 2), "s..."
    )
    Sys.sleep(wait)
  }
  
  stop("SEC request failed after retries: ", conditionMessage(last_err))
}

sec_get_json <- function(url) {
  req <- request(url) |>
    req_user_agent(sec_user_agent) |>
    req_headers(`Accept-Encoding` = "gzip, deflate") |>
    req_timeout(60)
  
  resp <- sec_perform_with_retry(req, max_tries = 5L, base_sleep = 1.0)
  fromJSON(resp_body_string(resp), simplifyVector = TRUE)
}

sec_download <- function(url, dest) {
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
  
  req <- request(url) |>
    req_user_agent(sec_user_agent) |>
    req_headers(`Accept-Encoding` = "gzip, deflate") |>
    req_timeout(120)
  
  resp <- sec_perform_with_retry(req, max_tries = 5L, base_sleep = 1.0)
  writeBin(resp_body_raw(resp), dest)
  TRUE
}

# -----------------------
# 3) Load SEC MF map (ticker -> cik/series/class)
# -----------------------
load_sec_mf_map <- function(sec_get_json,
                            url = "https://www.sec.gov/files/company_tickers_mf.json") {
  
  x <- sec_get_json(url)
  
  if (is.data.frame(x)) {
    dt <- as.data.table(x)
  } else if (is.list(x) && all(vapply(x, is.list, logical(1)))) {
    dt <- rbindlist(x, fill = TRUE, use.names = TRUE)
  } else if (is.list(x) && all(c("fields", "data") %in% names(x))) {
    dt <- as.data.table(x$data)
    if (length(x$fields) == ncol(dt)) setnames(dt, x$fields)
  } else {
    stop("SEC MF map JSON parsed into an unexpected structure. Try: str(x)")
  }
  
  setnames(dt, tolower(names(dt)))
  
  ticker_cols <- names(dt)[grepl("ticker|symbol", names(dt), ignore.case = TRUE)]
  if (length(ticker_cols) == 0) {
    stop("No ticker/symbol column found. Columns: ", paste(names(dt), collapse = ", "))
  }
  ticker_col <- if ("ticker" %in% ticker_cols) "ticker" else if ("symbol" %in% ticker_cols) "symbol" else ticker_cols[1]
  if (ticker_col != "ticker") setnames(dt, ticker_col, "ticker")
  
  if (!"cik" %in% names(dt)) {
    cik_cols <- names(dt)[grepl("^cik$|cik_str|ciknumber", names(dt), ignore.case = TRUE)]
    if (length(cik_cols)) setnames(dt, cik_cols[1], "cik")
  }
  if (!"seriesid" %in% names(dt)) {
    s_cols <- names(dt)[grepl("series", names(dt), ignore.case = TRUE)]
    if (length(s_cols)) setnames(dt, s_cols[1], "seriesid")
  }
  if (!"classid" %in% names(dt)) {
    c_cols <- names(dt)[grepl("class", names(dt), ignore.case = TRUE)]
    if (length(c_cols)) setnames(dt, c_cols[1], "classid")
  }
  
  dt[, ticker := toupper(trimws(as.character(ticker)))]
  if ("cik" %in% names(dt)) dt[, cik := as.integer(cik)]
  dt
}

message("Loading SEC MF map...")
mf_map <- load_sec_mf_map(sec_get_json)
message("SEC MF map loaded. Columns: ", paste(names(mf_map), collapse = ", "))
print(head(mf_map[, .(ticker, cik, seriesid, classid)], 5))

# -----------------------
# 4) Join TSR list to SEC identifiers
# -----------------------
tsr_dt[, ticker := toupper(trimws(ticker))]

fund_master <- unique(tsr_dt[, .(crsp_portno, fund_name, ticker)], by = "crsp_portno")
tsr_merge   <- tsr_dt[!(is.na(ticker) | ticker == "")]

mf_map <- unique(as.data.table(mf_map)[, .(ticker, cik, seriesid, classid)])
dt <- merge(tsr_merge, mf_map, by = "ticker", all.x = TRUE)

message("Unique funds in TSR list (crsp_portno): ", fund_master[, uniqueN(crsp_portno)])
message("Rows in tsr_list with non-empty ticker: ", nrow(dt))
message("Rows matched to CIK: ", dt[!is.na(cik), .N])
message("Rows NOT matched (missing CIK): ", dt[is.na(cik), .N])

todo_all <- unique(dt[!is.na(cik), .(crsp_portno, fund_name, ticker, cik, seriesid, classid)])
setorder(todo_all, ticker)

# -----------------------
# 5) Limit size for debugging (OPTIONAL)
# -----------------------
max_funds <- Inf  # set to 10 for testing; Inf = FULL sample of CIK-matched funds
if (is.finite(max_funds)) todo <- todo_all[1:min(.N, max_funds)] else todo <- todo_all

message("Funds to process in this run (CIK-matched): ", nrow(todo))
if (nrow(todo) == 0) stop("No funds to process (no CIK matches in this run).")

# -----------------------
# 6) Helpers
# -----------------------
pad10 <- function(x) sprintf("%010d", as.integer(x))

get_ncsr_filings <- function(cik_num, filed_on_or_after = as.Date("2024-07-24")) {
  cik10 <- pad10(cik_num)
  sub_url <- paste0("https://data.sec.gov/submissions/CIK", cik10, ".json")
  
  sub <- sec_get_json(sub_url)
  recent <- sub$filings$recent
  if (is.null(recent) || is.null(recent$form)) return(data.table())
  
  f <- data.table(
    form       = recent$form,
    filingDate = as.Date(recent$filingDate),
    reportDate = suppressWarnings(as.Date(recent$reportDate)),
    accession  = recent$accessionNumber,
    primaryDoc = recent$primaryDocument
  )
  
  f <- f[form %chin% c("N-CSR", "N-CSR/A")]
  f <- f[!is.na(filingDate) & filingDate >= filed_on_or_after]
  setorder(f, -filingDate)
  f
}

fetch_filing_index_items <- function(cik_num, accession) {
  cik_dir    <- as.character(as.integer(cik_num))
  acc_nodash <- gsub("-", "", accession)
  
  url1 <- paste0("https://www.sec.gov/Archives/edgar/data/", cik_dir, "/", acc_nodash, "/index.json")
  url2 <- paste0("https://www.sec.gov/Archives/edgar/data/", cik_dir, "/", acc_nodash, "/", accession, "-index.json")
  
  idx <- tryCatch(sec_get_json(url1), error = function(e) NULL)
  if (is.null(idx)) idx <- tryCatch(sec_get_json(url2), error = function(e) NULL)
  if (is.null(idx)) return(data.table())
  
  items <- idx$directory$item
  if (is.null(items)) return(data.table())
  as.data.table(items)
}

pick_candidate_docs <- function(items_dt, primary_doc = NULL) {
  if (nrow(items_dt) == 0) return(data.table())
  
  docs <- copy(items_dt)
  setnames(docs, tolower(names(docs)))
  if (!"name" %in% names(docs)) return(data.table())
  
  docs[, name_l := tolower(name)]
  docs[, score := 0L]
  
  docs[grepl("tailored|shareholder|shrhld|annual|semi.?annual|report", name_l), score := score + 10L]
  docs[grepl("ncsr", name_l), score := score + 3L]
  docs[grepl("\\.pdf$", name_l), score := score + 5L]
  docs[grepl("\\.htm|\\.html$", name_l), score := score + 2L]
  
  docs[grepl("\\.xml|\\.xsd|\\.jpg|\\.png|\\.gif|\\.zip$", name_l), score := score - 10L]
  
  if (!is.null(primary_doc) && !is.na(primary_doc) && nzchar(primary_doc)) {
    docs[tolower(name) == tolower(primary_doc), score := score + 50L]
  }
  
  docs <- docs[score > 0]
  if (nrow(docs) == 0) return(data.table())
  
  docs[, size_num := suppressWarnings(as.numeric(size))]
  setorder(docs, -score, -size_num)
  
  docs[1:min(3, .N), .(name, type, size, score)]
}

safe_filename <- function(x) {
  x <- stringi::stri_trans_general(x, "Latin-ASCII")
  x <- gsub("[^A-Za-z0-9._-]+", "_", x)
  substr(x, 1, 180)
}

download_html_assets <- function(html_path, cik_dir, acc_nodash, filing_dir) {
  if (!file.exists(html_path)) return(invisible(NULL))
  ext <- tolower(tools::file_ext(html_path))
  if (!ext %chin% c("htm", "html")) return(invisible(NULL))
  
  txt <- paste(readLines(html_path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
  
  m <- gregexpr("(?:src|href)=[\"']([^\"']+)[\"']", txt, perl = TRUE)
  hits <- regmatches(txt, m)[[1]]
  if (length(hits) == 0) return(invisible(NULL))
  
  urls <- sub("^(?:src|href)=[\"']([^\"']+)[\"']$", "\\1", hits, perl = TRUE)
  urls <- unique(urls)
  
  urls <- urls[!grepl("^(https?:)?//", urls)]
  urls <- urls[!grepl("^(#|mailto:|javascript:|data:)", urls)]
  urls <- sub("\\?.*$", "", urls)
  urls <- urls[urls != ""]
  
  keep <- grepl("\\.(css|js|png|jpg|jpeg|gif|svg|webp|woff|woff2|ttf)$", tolower(urls))
  urls <- urls[keep]
  if (length(urls) == 0) return(invisible(NULL))
  
  urls <- gsub("^/+", "", urls)
  urls <- gsub("\\.\\./", "", urls)
  
  for (u in urls) {
    asset_url  <- paste0("https://www.sec.gov/Archives/edgar/data/", cik_dir, "/", acc_nodash, "/", u)
    asset_dest <- file.path(filing_dir, u)
    if (!file.exists(asset_dest)) {
      try(sec_download(asset_url, asset_dest), silent = TRUE)
    }
  }
  invisible(NULL)
}

# -----------------------
# 6B) Relevance filter helpers
# -----------------------
use_relevance_filter <- TRUE

read_text_safe <- function(path, max_chars = 2e6) {
  if (!file.exists(path)) return("")
  x <- tryCatch(readLines(path, warn = FALSE, encoding = "UTF-8"), error = function(e) character(0))
  if (length(x) == 0) return("")
  txt <- paste(x, collapse = "\n")
  if (nchar(txt, type = "chars") > max_chars) substr(txt, 1, max_chars) else txt
}

download_filing_summary_xml <- function(items_dt, cik_dir, acc_nodash, filing_dir) {
  items <- copy(items_dt)
  setnames(items, tolower(names(items)))
  if (!"name" %in% names(items)) return(NA_character_)
  items[, name_l := tolower(name)]
  
  if (!any(items$name_l == "filingsummary.xml")) return(NA_character_)
  
  xml_name <- items[name_l == "filingsummary.xml", name][1]
  xml_url  <- paste0("https://www.sec.gov/Archives/edgar/data/", cik_dir, "/", acc_nodash, "/", xml_name)
  xml_dest <- file.path(filing_dir, "FilingSummary.xml")
  
  if (!file.exists(xml_dest)) {
    try(sec_download(xml_url, xml_dest), silent = TRUE)
  }
  if (file.exists(xml_dest)) return(xml_dest)
  NA_character_
}

extract_fund_keywords <- function(fund_name, k = 3) {
  if (is.na(fund_name) || !nzchar(fund_name)) return(character(0))
  s <- toupper(fund_name)
  s <- gsub("[^A-Z0-9 ]+", " ", s)
  words <- unlist(strsplit(s, "\\s+"))
  words <- words[nzchar(words)]
  
  stopw <- c(
    "FUND","FUNDS","TRUST","SERIES","PORTFOLIO","PORTFOLIOS","INC","LLC","LTD",
    "CLASS","SHARES","SHARE","INVESTOR","INSTITUTIONAL","ADVISOR","ADVISORS",
    "THE","AND","OF","A","AN","TO","FOR"
  )
  words <- words[!(words %chin% stopw)]
  words <- words[nchar(words) >= 5]
  words <- unique(words)
  
  if (length(words) == 0) return(character(0))
  words[1:min(k, length(words))]
}

match_in_folder <- function(filing_dir, high_patterns = character(0), low_patterns = character(0)) {
  files <- list.files(filing_dir, pattern = "\\.(htm|html|xml)$", full.names = TRUE, recursive = TRUE)
  files <- unique(c(file.path(filing_dir, "FilingSummary.xml"), files))
  files <- files[file.exists(files)]
  
  if (length(files) == 0) return(NA)
  
  hp <- unique(toupper(high_patterns[nzchar(high_patterns)]))
  lp <- unique(toupper(low_patterns[nzchar(low_patterns)]))
  
  if (length(hp) == 0 && length(lp) == 0) return(NA)
  
  for (fp in files) {
    txt <- read_text_safe(fp)
    if (!nzchar(txt)) next
    tu <- toupper(txt)
    
    if (length(hp) > 0) {
      for (p in hp) if (grepl(p, tu, fixed = TRUE)) return(TRUE)
    }
    if (length(lp) > 0) {
      for (p in lp) if (grepl(p, tu, fixed = TRUE)) return(TRUE)
    }
  }
  
  if (length(hp) > 0) return(FALSE)
  NA
}

match_in_dirs <- function(dirs, high_patterns = character(0), low_patterns = character(0)) {
  dirs <- dirs[dir.exists(dirs)]
  if (length(dirs) == 0) return(NA)
  
  for (d in dirs) {
    out <- match_in_folder(d, high_patterns = high_patterns, low_patterns = low_patterns)
    if (isTRUE(out)) return(TRUE)
    if (isFALSE(out)) return(FALSE)
  }
  NA
}

# -----------------------
# 6C) Resume: load existing logs
# -----------------------
log_detail <- data.table()
existing_fund_log <- data.table()

if (!isTRUE(fresh_start)) {
  if (file.exists(log_file_detail)) {
    log_detail <- tryCatch(fread(log_file_detail), error = function(e) data.table())
    message("Loaded existing detail log rows: ", nrow(log_detail))
  }
  
  if (file.exists(log_file_fund)) {
    existing_fund_log <- tryCatch(fread(log_file_fund), error = function(e) data.table())
    message("Loaded existing fund log rows: ", nrow(existing_fund_log))
  }
} else {
  message("fresh_start=TRUE: skipping log load (no resume).")
}

# -----------------------
# 6D) Resume: skip-set only from confirmed MATCHED_THIS_FUND
# -----------------------
evaluated_key_set <- character(0)

if (!isTRUE(fresh_start) && file.exists(log_file_detail)) {
  tmp <- tryCatch(fread(log_file_detail), error = function(e) data.table())
  if (nrow(tmp) > 0 && all(c("crsp_portno","cik","accession","status") %in% names(tmp))) {
    tmp <- tmp[status == "MATCHED_THIS_FUND" & !is.na(crsp_portno) & !is.na(cik) & nzchar(accession)]
    evaluated_key_set <- unique(paste(tmp$crsp_portno, tmp$cik, tmp$accession, sep = "|"))
  }
}

if (isTRUE(fresh_start)) {
  evaluated_key_set <- character(0)
}

# -----------------------
# 7) Main loop
# -----------------------
download_latest_only <- FALSE
filed_cutoff <- as.Date("2024-07-24")

fund_run_ids <- unique(todo$crsp_portno)

fund_summary <- fund_master[, .(
  crsp_portno, ticker, fund_name,
  cik = NA_integer_,
  n_filings_found = 0L,
  n_filings_downloaded = 0L,
  downloaded_any = 0L,
  status = "",
  note = ""
)]
setkey(fund_summary, crsp_portno)

if (nrow(existing_fund_log) > 0 && "crsp_portno" %in% names(existing_fund_log)) {
  keep_cols <- intersect(
    names(existing_fund_log),
    c("crsp_portno","cik","n_filings_found","n_filings_downloaded","downloaded_any","status","note")
  )
  tmp <- unique(as.data.table(existing_fund_log)[, ..keep_cols], by = "crsp_portno")
  setkey(tmp, crsp_portno)
  fund_summary[tmp, `:=`(
    cik                  = i.cik,
    n_filings_found      = i.n_filings_found,
    n_filings_downloaded = i.n_filings_downloaded,
    downloaded_any       = i.downloaded_any,
    status               = i.status,
    note                 = i.note
  )]
}

for (i in seq_len(nrow(todo))) {
  ctrl <- check_pause_stop()
  if (identical(ctrl, "STOP")) {
    message("STOP requested. Writing checkpoints and exiting.")
    break
  }
  
  row <- todo[i]
  fund_summary[.(row$crsp_portno), cik := as.integer(row$cik)]
  
  if (fund_summary[.(row$crsp_portno), status] == "DONE") {
    message(sprintf(
      "[%s] SKIP (DONE) %d/%d  ticker=%s  cik=%s",
      format(Sys.time(), "%H:%M:%S"), i, nrow(todo), row$ticker, row$cik
    ))
    next
  }
  
  message(sprintf(
    "[%s] Fund %d/%d  ticker=%s  cik=%s",
    format(Sys.time(), "%H:%M:%S"), i, nrow(todo), row$ticker, row$cik
  ))
  
  filings <- tryCatch(
    get_ncsr_filings(row$cik, filed_on_or_after = filed_cutoff),
    error = function(e) data.table(error = as.character(e))
  )
  
  if ("error" %in% names(filings)) {
    fund_summary[.(row$crsp_portno), `:=`(status="SUBMISSIONS_ERROR", note=filings$error)]
    next
  }
  
  if (nrow(filings) == 0) {
    fund_summary[.(row$crsp_portno),
                 `:=`(status="NO_NCSR_FOUND", note=paste0("No N-CSR/N-CSR/A on/after ", filed_cutoff))]
    next
  }
  
  fund_summary[.(row$crsp_portno), n_filings_found := nrow(filings)]
  if (download_latest_only) filings <- filings[1]
  
  filing_successes_for_this_fund <- 0L
  
  for (k in seq_len(nrow(filings))) {
    ctrl <- check_pause_stop()
    if (identical(ctrl, "STOP")) {
      message("STOP requested. Breaking out of filing loop.")
      break
    }
    
    acc  <- filings$accession[k]
    fdt  <- filings$filingDate[k]
    rdt  <- filings$reportDate[k]
    pdoc <- filings$primaryDoc[k]
    
    acc_key <- paste(row$crsp_portno, as.integer(row$cik), acc, sep = "|")
    if (acc_key %chin% evaluated_key_set) {
      message("  Filing already CONFIRMED MATCHED earlier. Skipping accession: ", acc)
      next
    }
    
    period_dt  <- if (!is.na(rdt)) rdt else fdt
    period_lab <- format(period_dt, "%Y-%m-%d")
    
    message("  Filing: ", acc, "  filed: ", fdt, "  reportDate: ", rdt, "  primaryDoc: ", pdoc)
    
    items_dt <- tryCatch(fetch_filing_index_items(row$cik, acc),
                         error = function(e) data.table())
    if (nrow(items_dt) == 0) {
      log_detail <- rbind(
        log_detail,
        data.table(
          crsp_portno=row$crsp_portno, ticker=row$ticker, fund_name=row$fund_name,
          cik=row$cik, accession=acc, filingDate=fdt, reportDate=rdt,
          status="INDEX_ERROR", detail="Could not fetch index.json for filing directory"
        ),
        fill=TRUE
      )
      fwrite(log_detail, log_file_detail)
      next
    }
    
    candidates <- pick_candidate_docs(items_dt, primary_doc = pdoc)
    if (nrow(candidates) == 0) {
      log_detail <- rbind(
        log_detail,
        data.table(
          crsp_portno=row$crsp_portno, ticker=row$ticker, fund_name=row$fund_name,
          cik=row$cik, accession=acc, filingDate=fdt, reportDate=rdt,
          status="NO_CANDIDATE_DOCS", detail="Index fetched but no likely report docs found"
        ),
        fill=TRUE
      )
      fwrite(log_detail, log_file_detail)
      next
    }
    
    cik_dir    <- as.character(as.integer(row$cik))
    acc_nodash <- gsub("-", "", acc)
    
    filing_dir_local <- file.path(out_raw, paste0("CIK", pad10(row$cik)), period_lab, acc_nodash)
    dir.create(filing_dir_local, recursive = TRUE, showWarnings = FALSE)
    
    for (j in seq_len(nrow(candidates))) {
      ctrl <- check_pause_stop()
      if (identical(ctrl, "STOP")) {
        message("STOP requested during downloads. Breaking out.")
        break
      }
      
      docname <- candidates$name[j]
      doc_url <- paste0("https://www.sec.gov/Archives/edgar/data/", cik_dir, "/", acc_nodash, "/", docname)
      dest <- file.path(filing_dir_local, safe_filename(docname))
      
      if (file.exists(dest)) {
        log_detail <- rbind(
          log_detail,
          data.table(
            crsp_portno=row$crsp_portno, ticker=row$ticker, fund_name=row$fund_name,
            cik=row$cik, accession=acc, filingDate=fdt, reportDate=rdt,
            status="ALREADY_EXISTS", detail=dest
          ),
          fill=TRUE
        )
        next
      }
      
      message("    Downloading: ", docname)
      ok <- tryCatch(sec_download(doc_url, dest), error = function(e) e)
      
      if (isTRUE(ok)) {
        download_html_assets(dest, cik_dir, acc_nodash, filing_dir_local)
        log_detail <- rbind(
          log_detail,
          data.table(
            crsp_portno=row$crsp_portno, ticker=row$ticker, fund_name=row$fund_name,
            cik=row$cik, accession=acc, filingDate=fdt, reportDate=rdt,
            status="DOWNLOADED", detail=dest
          ),
          fill=TRUE
        )
      } else {
        log_detail <- rbind(
          log_detail,
          data.table(
            crsp_portno=row$crsp_portno, ticker=row$ticker, fund_name=row$fund_name,
            cik=row$cik, accession=acc, filingDate=fdt, reportDate=rdt,
            status="DOWNLOAD_ERROR", detail=as.character(ok)
          ),
          fill=TRUE
        )
      }
      
      fwrite(log_detail, log_file_detail)
    }
    
    if (use_relevance_filter) {
      xml_path <- download_filing_summary_xml(items_dt, cik_dir, acc_nodash, filing_dir_local)
      if (!is.na(xml_path)) {
        log_detail <- rbind(
          log_detail,
          data.table(
            crsp_portno=row$crsp_portno, ticker=row$ticker, fund_name=row$fund_name,
            cik=row$cik, accession=acc, filingDate=fdt, reportDate=rdt,
            status="GOT_FILINGSUMMARY_XML", detail=xml_path
          ),
          fill=TRUE
        )
        fwrite(log_detail, log_file_detail)
      }
    }
    
    if (use_relevance_filter) {
      fund_kw <- extract_fund_keywords(row$fund_name, k = 3)
      
      high_fund <- c(row$classid, row$seriesid, row$ticker)
      high_fund <- high_fund[!is.na(high_fund) & nzchar(high_fund)]
      
      filing_dir_gdrive <- file.path(gdrive_root, paste0("CIK", pad10(row$cik)), period_lab, acc_nodash)
      
      match_fund <- match_in_dirs(
        dirs = c(filing_dir_local, filing_dir_gdrive),
        high_patterns = high_fund,
        low_patterns  = fund_kw
      )
      
      if (isTRUE(match_fund)) {
        filing_successes_for_this_fund <- filing_successes_for_this_fund + 1L
        log_detail <- rbind(
          log_detail,
          data.table(
            crsp_portno=row$crsp_portno, ticker=row$ticker, fund_name=row$fund_name,
            cik=row$cik, accession=acc, filingDate=fdt, reportDate=rdt,
            status="MATCHED_THIS_FUND",
            detail=paste0("Matched by class/series/ticker or fund keywords: ", paste(fund_kw, collapse = ", "))
          ),
          fill=TRUE
        )
        
        evaluated_key_set <- unique(c(evaluated_key_set, acc_key))
        
      } else if (isFALSE(match_fund)) {
        log_detail <- rbind(
          log_detail,
          data.table(
            crsp_portno=row$crsp_portno, ticker=row$ticker, fund_name=row$fund_name,
            cik=row$cik, accession=acc, filingDate=fdt, reportDate=rdt,
            status="NOT_THIS_FUND",
            detail="Folder exists/downloaded but did NOT match this fund's class/series/ticker."
          ),
          fill=TRUE
        )
      } else {
        log_detail <- rbind(
          log_detail,
          data.table(
            crsp_portno=row$crsp_portno, ticker=row$ticker, fund_name=row$fund_name,
            cik=row$cik, accession=acc, filingDate=fdt, reportDate=rdt,
            status="UNVERIFIED_MATCH",
            detail="Could not verify match (no readable HTML/XML hit). Kept folder; not counted."
          ),
          fill=TRUE
        )
      }
      
      fwrite(log_detail, log_file_detail)
      
    } else {
      filing_successes_for_this_fund <- filing_successes_for_this_fund + 1L
    }
    
    maybe_move_cik_batch(out_raw, gdrive_root, exclude_cik10 = pad10(row$cik), batch_n = 25L)
  }
  
  if (identical(ctrl, "STOP")) {
    fund_summary[.(row$crsp_portno),
                 `:=`(
                   n_filings_downloaded = filing_successes_for_this_fund,
                   downloaded_any = as.integer(filing_successes_for_this_fund > 0),
                   status = "STOPPED_MID_RUN",
                   note = "STOP file detected; rerun script to resume."
                 )]
    break
  }
  
  fund_summary[.(row$crsp_portno),
               `:=`(
                 n_filings_downloaded = filing_successes_for_this_fund,
                 downloaded_any = as.integer(filing_successes_for_this_fund > 0),
                 status = "DONE",
                 note = ""
               )]
  
  if (i %% 5 == 0) {
    fwrite(log_detail, log_file_detail)
    fwrite(fund_summary, log_file_fund, bom = TRUE)
    message("Checkpoint written.")
  }
}

# -----------------------
# 8) Fund-wise log + pending counts
# -----------------------
fund_summary[is.na(status) | status == "", `:=`(
  status = fifelse(crsp_portno %in% fund_run_ids, "NOT_MATCHED_OR_SKIPPED", "NOT_RUN"),
  note   = fifelse(is.na(ticker) | ticker == "", "Missing/blank ticker in tsr_list", note)
)]

fund_summary[, download_rate := fifelse(
  n_filings_found > 0,
  round(100 * n_filings_downloaded / n_filings_found, 2),
  as.numeric(NA)
)]

todo_ids     <- unique(todo$crsp_portno)
n_total_todo <- length(todo_ids)
n_done       <- fund_summary[crsp_portno %in% todo_ids & status == "DONE", uniqueN(crsp_portno)]
n_pending    <- n_total_todo - n_done

total_funds         <- fund_master[, uniqueN(crsp_portno)]
funds_with_any      <- fund_summary[downloaded_any == 1, uniqueN(crsp_portno)]
pct_funds_with_any  <- round(100 * funds_with_any / total_funds, 2)

total_filings_found <- sum(fund_summary$n_filings_found, na.rm = TRUE)
total_filings_dl    <- sum(fund_summary$n_filings_downloaded, na.rm = TRUE)
pct_filings_dl      <- if (total_filings_found > 0) round(100 * total_filings_dl / total_filings_found, 2) else NA_real_

fund_summary[, `:=`(
  total_unique_funds_in_tsr_list = total_funds,
  funds_with_any_download        = funds_with_any,
  pct_funds_with_any_download    = pct_funds_with_any,
  total_filings_found_all_funds  = total_filings_found,
  total_filings_downloaded_all   = total_filings_dl,
  pct_filings_downloaded_all     = pct_filings_dl,
  run_total_todo_funds           = n_total_todo,
  run_funds_completed            = n_done,
  run_funds_pending              = n_pending
)]

setorder(fund_summary, -downloaded_any, -n_filings_downloaded, ticker, crsp_portno)

fwrite(fund_summary, log_file_fund, bom = TRUE)
fwrite(log_detail, log_file_detail)

repeat {
  moved <- maybe_move_cik_batch(out_raw, gdrive_root, exclude_cik10 = NULL, batch_n = 25L)
  if (!isTRUE(moved)) break
}

message("DONE.")
message("Fund-wise log saved to:  ", normalizePath(log_file_fund, winslash = "/"))
message("Detail log saved to:     ", normalizePath(log_file_detail, winslash = "/"))
message("Move log saved to:       ", normalizePath(log_file_move, winslash = "/"))
message("Local files under:       ", normalizePath(out_raw, winslash = "/"))
message("Google Drive target:     ", gdrive_root)
message("RUN pending collections: ", n_pending, " (0 means complete).")
