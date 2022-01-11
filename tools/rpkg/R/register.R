# helper to clean up non-utf and posixlt vectors
encode_values <- function(value) {
  value <- as.data.frame(value)
  names(value) <- enc2utf8(names(value))

  is_character <- vapply(value, is.character, logical(1))
  value[is_character] <- lapply(value[is_character], enc2utf8)
  is_factor <- vapply(value, is.factor, logical(1))
  value[is_factor] <- lapply(value[is_factor], function(x) {
    levels(x) <- enc2utf8(levels(x))
    x
   })

  is_posixlt <- vapply(value, inherits, "POSIXlt", FUN.VALUE = logical(1))
  value[is_posixlt] <- lapply(value[is_posixlt], as.POSIXct)
  value
}

#' Register a data frame as a virtual table
#'
#' `duckdb_register()` registers a data frame as a virtual table (view)
#'  in a DuckDB connection.
#'  No data is copied.
#'
#' `duckdb_unregister()` unregisters a previously registered data frame.
#' @param conn A DuckDB connection, created by `dbConnect()`.
#' @param name The name for the virtual table that is registered or unregistered
#' @param df A `data.frame` with the data for the virtual table
#' @return These functions are called for their side effect.
#' @export
#' @examples
#' con <- dbConnect(duckdb())
#'
#' data <- data.frame(a = 1:3, b = letters[1:3])
#'
#' duckdb_register(con, "data", data)
#' dbReadTable(con, "data")
#'
#' duckdb_unregister(con, "data")
#' try(dbReadTable(con, "data"))
#'
#' dbDisconnect(con)
duckdb_register <- function(conn, name, df) {
  stopifnot(dbIsValid(conn))
  df <- encode_values(as.data.frame(df))
  .Call(`_duckdb_register_R`, conn@conn_ref, enc2utf8(as.character(name)), df)
  invisible(TRUE)
}

#' @rdname duckdb_register
#' @export
duckdb_unregister <- function(conn, name) {
  stopifnot(dbIsValid(conn))
  .Call(`_duckdb_unregister_R`, conn@conn_ref, enc2utf8(as.character(name)))
  invisible(TRUE)
}

#' Register an Arrow data source as a virtual table
#'
#' `duckdb_register_arrow()` registers an Arrow data source as a virtual table (view)
#'  in a DuckDB connection.
#'  No data is copied.
#'
#' `duckdb_unregister_arrow()` unregisters a previously registered data frame.
#' @param conn A DuckDB connection, created by `dbConnect()`.
#' @param name The name for the virtual table that is registered or unregistered
#' @param arrow_scannable A scannable Arrow-object
#' @param use_async Switched to the asynchronous scanner. default FALSE
#' @return These functions are called for their side effect.
#' @export
duckdb_register_arrow <- function(conn, name, arrow_scannable, use_async=FALSE) {
  stopifnot(dbIsValid(conn))

    # create some R functions to pass to c-land
    export_fun <- function(arrow_scannable, stream_ptr, projection=NULL, filter=TRUE) {
        arrow::Scanner$create(arrow_scannable, projection, filter, use_async=use_async)$ToRecordBatchReader()$export_to_c(stream_ptr)
    }
   # pass some functions to c land so we don't have to look them up there
   function_list <- list(export_fun, arrow::Expression$create, arrow::Expression$field_ref, arrow::Expression$scalar)
  .Call(`_duckdb_register_arrow_R`, conn@conn_ref, enc2utf8(as.character(name)), function_list, arrow_scannable)
  invisible(TRUE)
}

#' @rdname duckdb_register_arrow
#' @export
duckdb_unregister_arrow <- function(conn, name) {
  .Call(`_duckdb_unregister_arrow_R`, conn@conn_ref, enc2utf8(as.character(name)))
  invisible(TRUE)
}

#' @rdname duckdb_register_arrow
#' @export
duckdb_list_arrow <- function(conn) {
    sort(gsub("_registered_arrow_", "", names(attributes(conn@driver@database_ref)), fixed=TRUE))
}
