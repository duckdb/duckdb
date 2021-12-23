#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbAppendTable
#' @usage NULL
dbAppendTable__duckdb_connection <- function(conn, name, value, ..., row.names = NULL) {
  if (!is.null(row.names)) {
    stop("Can't pass `row.names` to `dbAppendTable()`")
  }

  target_names <- dbListFields(conn, name)

  if (!all(names(value) %in% target_names)) {
    stop("Column `", setdiff(names(value), target_names)[[1]], "` does not exist in target table.")
  }

  if (nrow(value)) {
    table_name <- dbQuoteIdentifier(conn, name)

    view_name <- sprintf("_duckdb_append_view_%s", duckdb_random_string())
    on.exit(duckdb_unregister(conn, view_name))
    duckdb_register(conn, view_name, value)

    sql <- paste0(
      "INSERT INTO ", table_name, "\n",
      "(", paste(dbQuoteIdentifier(conn, names(value)), collapse = ", "), ")\n",
      "SELECT * FROM ", view_name
    )

    dbExecute(conn, sql)

    rs_on_connection_updated(conn, hint = paste0("Updated table'", table_name, "'"))
  }

  invisible(nrow(value))
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbAppendTable", "duckdb_connection", dbAppendTable__duckdb_connection)
