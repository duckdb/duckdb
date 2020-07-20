duckdb_connection <- function(duckdb_driver, debug) {
  new(
    "duckdb_connection",
    conn_ref = .Call(duckdb_connect_R, duckdb_driver@database_ref),
    driver = duckdb_driver,
    debug = debug
  )
}

#' Register a R data.frame as a virtual table (view) in DuckDB without copying the data
#' @rdname duckdb_connection
#' @param conn A DuckDB connection, created by `dbConnect()`.
#' @param name The name for the virtual table that is registered
#' @param df A `data.frame` with the data for the virtual table
#' @export
duckdb_register <- function(conn, name, df) {
  stopifnot(dbIsValid(conn))
  .Call(duckdb_register_R, conn@conn_ref, as.character(name), as.data.frame(df))
  invisible(TRUE)
}

#' Unregister a virtual table referring to a data.frame
#' @rdname duckdb_connection
#' @param conn A DuckDB connection, created by `dbConnect()`.
#' @param name The name for the virtual table previously registered using `duckdb_register()`.
#' @export
duckdb_unregister <- function(conn, name) {
  stopifnot(dbIsValid(conn))
  .Call(duckdb_unregister_R, conn@conn_ref, as.character(name))
  invisible(TRUE)
}

#' DuckDB connection class
#' @rdname duckdb_connection
#' @export
setClass(
  "duckdb_connection",
  contains = "DBIConnection",
  slots = list(dbdir = "character", conn_ref = "externalptr", driver = "duckdb_driver", debug = "logical")
)

#' @rdname duckdb_connection
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "duckdb_connection",
  function(object) {
    cat(sprintf("<duckdb_connection %s driver=%s>\n", extptr_str(object@conn_ref), drv_to_string(object@driver)))
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "duckdb_connection",
  function(dbObj, ...) {
    valid <- FALSE
    tryCatch(
      {
        dbGetQuery(dbObj, SQL("SELECT 1"))
        valid <- TRUE
      },
      error = function(c) {
      }
    )
    valid
  }
)

#' @rdname duckdb_connection
#' @param shutdown Shut down the DuckDB database instance that this connection refers to.
#' @export
setMethod(
  "dbDisconnect", "duckdb_connection",
  function(conn, ..., shutdown = FALSE) {
    if (!dbIsValid(conn)) {
      warning("Connection already closed.", call. = FALSE)
    }
    .Call(duckdb_disconnect_R, conn@conn_ref)
    if (shutdown) {
      duckdb_shutdown(conn@driver)
    }

    invisible(TRUE)
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbSendQuery
#' @export
setMethod(
  "dbSendQuery", c("duckdb_connection", "character"),
  function(conn, statement, ...) {
    if (conn@debug) {
      cat("Q ", statement, "\n")
    }
    statement <- enc2utf8(statement)
    stmt_lst <- .Call(duckdb_prepare_R, conn@conn_ref, statement)

    res <- duckdb_result(
      connection = conn,
      stmt_lst = stmt_lst
    )
    params <- list(...)
    if (length(params) == 1 && class(params[[1]])[[1]] == "list") {
      params <- params[[1]]
    }
    if (length(params) > 0) {
      dbBind(res, params)
    }
    return(res)
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", "duckdb_connection",
  function(dbObj, obj, ...) {
    dbDataType(dbObj@driver, obj, ...)
  }
)

duckdb_random_string <- function(x) {
  paste(sample(letters, 10, replace = TRUE), collapse = "")
}

#' @rdname duckdb_connection
#' @inheritParams DBI::dbWriteTable
#' @param row.names Whether the row.names of the data.frame should be preserved
#' @param overwrite If a table with the given name already exists, should it be overwritten?
#' @param append If a table with the given name already exists, just try to append the passed data to it
#' @param field.types Override the auto-generated SQL types
#' @param temporary Should the created table be temporary?
#' @export
setMethod(
  "dbWriteTable", c("duckdb_connection", "character", "data.frame"),
  function(conn,
           name,
           value,
           row.names = FALSE,
           overwrite = FALSE,
           append = FALSE,
           field.types = NULL,
           temporary = FALSE,
           ...) {
    check_flag(overwrite)
    check_flag(append)
    check_flag(temporary)

    # TODO: start a transaction if one is not already running

    if (overwrite && append) {
      stop("Setting both overwrite and append makes no sense")
    }

    # oof
    if (!is.null(field.types) &&
      (
        !is.character(field.types) ||
          any(is.na(names(field.types))) ||
          length(unique(names(field.types))) != length(names(field.types)) ||
          append
      )) {
      stop("invalid field.types argument")
    }
    value <- as.data.frame(value)
    if (!is.data.frame(value)) {
      stop("need a data frame as parameter")
    }

    # use Kirill's magic, convert rownames to additional column
    value <- sqlRownamesToColumn(value, row.names)

    if (dbExistsTable(conn, name)) {
      if (overwrite) {
        dbRemoveTable(conn, name)
      }
      if (!overwrite && !append) {
        stop(
          "Table ",
          name,
          " already exists. Set overwrite=TRUE if you want
                  to remove the existing table. Set append=TRUE if you would like to add the new data to the
                  existing table."
        )
      }
      if (append && any(names(value) != dbListFields(conn, name))) {
        stop("Column name mismatch for append")
      }
    }
    table_name <- dbQuoteIdentifier(conn, name)

    if (!dbExistsTable(conn, name)) {
      column_names <- dbQuoteIdentifier(conn, names(value))
      column_types <-
        vapply(value, dbDataType, dbObj = conn, FUN.VALUE = "character")

      if (!is.null(field.types)) {
        mapped_column_types <- field.types[names(value)]
        if (any(is.na(mapped_column_types)) ||
          length(mapped_column_types) != length(names(value))) {
          stop("Column name/type mismatch")
        }
        column_types <- mapped_column_types
      }

      temp_str <- ""
      if (temporary) temp_str <- "TEMPORARY"

      schema_str <- paste(column_names, column_types, collapse = ", ")
      dbExecute(conn, SQL(sprintf(
        "CREATE %s TABLE %s (%s)", temp_str, table_name, schema_str
      )))
    }

    if (length(value[[1]])) {
      classes <- unlist(lapply(value, function(v) {
        class(v)[[1]]
      }))
      for (c in names(classes[classes == "character"])) {
        value[[c]] <- enc2utf8(value[[c]])
      }
      for (c in names(classes[classes == "factor"])) {
        levels(value[[c]]) <- enc2utf8(levels(value[[c]]))
      }
    }
    view_name <- sprintf("_duckdb_append_view_%s", duckdb_random_string())
    on.exit(duckdb_unregister(conn, view_name))
    duckdb_register(conn, view_name, value)
    dbExecute(conn, sprintf("INSERT INTO %s SELECT * FROM %s", table_name, view_name))

    invisible(TRUE)
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbListTables
#' @export
setMethod(
  "dbListTables", "duckdb_connection",
  function(conn, ...) {
    dbGetQuery(
      conn,
      SQL(
        "SELECT name FROM sqlite_master() WHERE type='table' ORDER BY name"
      )
    )[[1]]
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbExistsTable
#' @export
setMethod(
  "dbExistsTable", c("duckdb_connection", "character"),
  function(conn, name, ...) {
    if (!dbIsValid(conn)) {
      stop("Invalid connection")
    }
    if (length(name) != 1) {
      stop("Can only have a single name argument")
    }
    exists <- FALSE
    tryCatch(
      {
        dbGetQuery(
          conn,
          sqlInterpolate(
            conn,
            "SELECT * FROM ? WHERE FALSE",
            dbQuoteIdentifier(conn, name)
          )
        )
        exists <- TRUE
      },
      error = function(c) {
      }
    )
    exists
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbListFields
#' @export
setMethod(
  "dbListFields", c("duckdb_connection", "character"),
  function(conn, name, ...) {
    names(dbGetQuery(
      conn,
      sqlInterpolate(
        conn,
        "SELECT * FROM ? WHERE FALSE",
        dbQuoteIdentifier(conn, name)
      )
    ))
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbRemoveTable
#' @export
setMethod(
  "dbRemoveTable", c("duckdb_connection", "character"),
  function(conn, name, ...) {
    dbExecute(
      conn,
      sqlInterpolate(conn, "DROP TABLE ?", dbQuoteIdentifier(conn, name))
    )
    invisible(TRUE)
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "duckdb_connection",
  function(dbObj, ...) {
    list(
      dbname = dbObj@dbdir,
      db.version = NA,
      username = NA,
      host = NA,
      port = NA
    )
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbBegin
#' @export
setMethod(
  "dbBegin", "duckdb_connection",
  function(conn, ...) {
    dbExecute(conn, SQL("BEGIN TRANSACTION"))
    invisible(TRUE)
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbCommit
#' @export
setMethod(
  "dbCommit", "duckdb_connection",
  function(conn, ...) {
    dbExecute(conn, SQL("COMMIT"))
    invisible(TRUE)
  }
)

#' @rdname duckdb_connection
#' @inheritParams DBI::dbRollback
#' @export
setMethod(
  "dbRollback", "duckdb_connection",
  function(conn, ...) {
    dbExecute(conn, SQL("ROLLBACK"))
    invisible(TRUE)
  }
)

#' Directly reads a CSV file into DuckDB, tries to detect and create the correct schema for it.
#' @rdname duckdb_connection
#' @param files One or more CSV file names, should all have the same structure though
#' @param tablename The database table the files should be read into
#' @param header Whether or not the CSV files have a separate header in the first line
#' @param na.strings Which strings in the CSV files should be considered to be NULL
#' @param nrow.check How many rows should be read from the CSV file to figure out data types
#' @param delim Which field separator should be used
#' @param quote Which quote character is used for columns in the CSV file
#' @param col.names Override the detected or generated column names
#' @param lower.case.names Transform column names to lower case
#' @param sep Alias for delim for compatibility
#' @param transaction Should a transaction be used for the entire operation
#' @export
read_csv_duckdb <- duckdb.read.csv <- function(conn, files, tablename, header = TRUE, na.strings = "", nrow.check = 500,
                                               delim = ",", quote = "\"", col.names = NULL, lower.case.names = FALSE, sep = delim, transaction = TRUE, ...) {

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

  tablename <- dbQuoteIdentifier(conn, tablename)

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
    thefile <- dbQuoteString(conn, encodeString(normalizePath(files[i])))
    dbExecute(conn, sprintf("COPY %s FROM %s (DELIMITER %s, QUOTE %s, HEADER %s, NULL %s)", tablename, thefile, dbQuoteString(conn, delim), dbQuoteString(conn, quote), tolower(header), dbQuoteString(conn, na.strings[1])))
  }
  dbGetQuery(conn, paste("SELECT COUNT(*) FROM", tablename))[[1]]

  if (transaction) {
    dbCommit(conn)
    on.exit(NULL)
  }
}
