#' Reads a CSV file into DuckDB
#'
#' Directly reads a CSV file into DuckDB, tries to detect and create the correct schema for it.
#' This usually is much faster than reading the data into R and writing it to DuckDB.
#'
#' @inheritParams duckdb_register
#' @param files One or more CSV file names, should all have the same structure though
#' @param header Whether or not the CSV files have a separate header in the first line
#' @param na.strings Which strings in the CSV files should be considered to be NULL
#' @param nrow.check How many rows should be read from the CSV file to figure out data types
#' @param delim Which field separator should be used
#' @param quote Which quote character is used for columns in the CSV file
#' @param col.names Override the detected or generated column names
#' @param lower.case.names Transform column names to lower case
#' @param sep Alias for delim for compatibility
#' @param transaction Should a transaction be used for the entire operation
#' @param ... Passed on to [read.csv()]
#' @return The number of rows in the resulted table, invisibly.
#' @export
#' @examples
#' con <- dbConnect(duckdb())
#'
#' data <- data.frame(a = 1:3, b = letters[1:3])
#' path <- tempfile(fileext = ".csv")
#'
#' write.csv(data, path, row.names = FALSE)
#'
#' duckdb_read_csv(con, "data", path)
#' dbReadTable(con, "data")
#'
#' dbDisconnect(con)
duckdb_read_csv <- function(conn, name, files, header = TRUE, na.strings = "", nrow.check = 500,
                            delim = ",", quote = "\"", col.names = NULL, lower.case.names = FALSE, sep = delim, transaction = TRUE, ...) {
  #
  if (length(na.strings) > 1) stop("na.strings must be of length 1")
  if (!missing(sep)) delim <- sep

  headers <- lapply(files, utils::read.csv, sep = delim, na.strings = na.strings, quote = quote, nrows = nrow.check, header = header, ...)
  if (length(files) > 1) {
    nn <- sapply(headers, ncol)
    if (!all(nn == nn[1])) stop("Files have different numbers of columns")
    nms <- sapply(headers, names)
    if (!all(nms == nms[, 1])) stop("Files have different variable names")
    types <- sapply(headers, function(df) sapply(df, dbDataType, dbObj = conn))
    if (!all(types == types[, 1])) stop("Files have different variable types")
  }

  if (transaction) {
    dbBegin(conn)
    on.exit(tryCatch(dbRollback(conn), error = function(e) {}))
  }

  tablename <- dbQuoteIdentifier(conn, name)

  if (!dbExistsTable(conn, tablename)) {
    if (lower.case.names) names(headers[[1]]) <- tolower(names(headers[[1]]))
    if (!is.null(col.names)) {
      if (lower.case.names) {
        warning("Ignoring lower.case.names parameter as overriding col.names are supplied.")
      }
      col.names <- as.character(col.names)
      if (length(unique(col.names)) != length(names(headers[[1]]))) {
        stop(
          "You supplied ", length(unique(col.names)), " unique column names, but file has ",
          length(names(headers[[1]])), " columns."
        )
      }
      names(headers[[1]]) <- col.names
    }
    dbWriteTable(conn, tablename, headers[[1]][FALSE, , drop = FALSE])
  }

  for (i in seq_along(files)) {
    thefile <- dbQuoteString(conn, enc2native(normalizePath(files[i])))
    dbExecute(conn, sprintf("COPY %s FROM %s (DELIMITER %s, QUOTE %s, HEADER %s, NULL %s)", tablename, thefile, dbQuoteString(conn, delim), dbQuoteString(conn, quote), tolower(header), dbQuoteString(conn, na.strings[1])))
  }
  out <- dbGetQuery(conn, paste("SELECT COUNT(*) FROM", tablename))[[1]]

  if (transaction) {
    dbCommit(conn)
    on.exit(NULL)
  }

  invisible(out)
}

#' Deprecated functions
#'
#' `read_csv_duckdb()` has been superseded by `duckdb_read_csv()`.
#' The order of the arguments has changed.
#'
#' @rdname deprecated
#' @export
#' @keywords internal
read_csv_duckdb <- function(conn, files, tablename, ...) {
  .Deprecated(
    "duckdb_read_csv",
    old = "read_csv_duckdb",
    msg = "Use 'duckdb_read_csv' instead, with changed order of arguments."
  )
  # Different order of arguments!
  duckdb_read_csv(conn, tablename, files, ...)
}
