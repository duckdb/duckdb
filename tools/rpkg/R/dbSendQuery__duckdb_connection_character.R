#' @rdname duckdb_connection-class
#' @inheritParams DBI::dbSendQuery
#' @inheritParams DBI::dbBind
#' @param arrow Whether the query should be returned as an Arrow Table
#' @usage NULL
dbSendQuery__duckdb_connection_character <- function(conn, statement, params = NULL, ..., arrow = FALSE) {
  if (conn@debug) {
    message("Q ", statement)
  }
  statement <- enc2utf8(statement)
  stmt_lst <- .Call(`_duckdb_prepare_R`, conn@conn_ref, statement)

  res <- duckdb_result(
    connection = conn,
    stmt_lst = stmt_lst,
    arrow = arrow
  )
  if (length(params) > 0) {
    dbBind(res, params)
  }
  return(res)
}

#' @rdname duckdb_connection-class
#' @export
setMethod("dbSendQuery", c("duckdb_connection", "character"), dbSendQuery__duckdb_connection_character)
