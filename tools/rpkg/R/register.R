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
  .Call(duckdb_register_R, conn@conn_ref, as.character(name), as.data.frame(df))
  invisible(TRUE)
}

#' @rdname duckdb_register
#' @export
duckdb_unregister <- function(conn, name) {
  stopifnot(dbIsValid(conn))
  .Call(duckdb_unregister_R, conn@conn_ref, as.character(name))
  invisible(TRUE)
}

#' @rdname duckdb_register
#' @export
duckdb_register_arrow <- function(conn, name, arrow_table) {
  stopifnot(dbIsValid(conn))
  record_batch_reader <- arrow::Scanner$create(arrow_table)$ToRecordBatchReader()

    # create a function to pass to c-land
    export_fun <- function(x) {
        record_batch_reader <- arrow::Scanner$create(x)$ToRecordBatchReader()
        stream_ptr <- arrow:::allocate_arrow_array_stream()
        arrow:::ExportRecordBatchReader(record_batch_reader, stream_ptr)
        return(stream_ptr)
    }
  .Call(duckdb_register_arrow_R, conn@conn_ref, as.character(name), export_fun, arrow_table)
  invisible(TRUE)
}

#' @rdname duckdb_register
#' @export
duckdb_unregister_arrow <- function(conn, name) {
  stopifnot(dbIsValid(conn))
  .Call(duckdb_unregister_arrow_R, conn@conn_ref, as.character(name))
  invisible(TRUE)
}