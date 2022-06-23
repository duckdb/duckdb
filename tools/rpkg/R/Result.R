#' DuckDB Result Set
#'
#' Methods for accessing result sets for queries on DuckDB connections.
#' Implements \linkS4class{DBIResult}.
#'
#' @aliases duckdb_result
#' @keywords internal
#' @export
setClass("duckdb_result",
  contains = "DBIResult",
  slots = list(
    connection = "duckdb_connection",
    stmt_lst = "list",
    env = "environment",
    arrow = "logical",
    query_result = "externalptr"
  )
)

duckdb_result <- function(connection, stmt_lst, arrow) {
  env <- new.env(parent = emptyenv())
  env$rows_fetched <- 0
  env$open <- TRUE
  env$rows_affected <- 0

  res <- new("duckdb_result", connection = connection, stmt_lst = stmt_lst, env = env, arrow = arrow)

  if (stmt_lst$n_param == 0) {
    if (arrow) {
      query_result <- duckdb_execute(res)
      new_res <- new("duckdb_result", connection = connection, stmt_lst = stmt_lst, env = env, arrow = arrow, query_result = query_result)
      return(new_res)
    } else {
      duckdb_execute(res)
    }
  }


  return(res)
}

duckdb_execute <- function(res) {
  out <- rapi_execute(res@stmt_lst$ref, res@arrow)
  duckdb_post_execute(res, out)
}

duckdb_post_execute <- function(res, out) {
  if (!res@arrow) {
    stopifnot(is.data.frame(out))

    if (!res@stmt_lst$type %in% c("SELECT", "EXPLAIN")) {
      res@env$rows_affected <- sum(as.numeric(out[[1]]))
    }

    res@env$resultset <- out
  }

  out
}

# as per is.integer documentation
is_wholenumber <- function(x, tol = .Machine$double.eps^0.5) abs(x - round(x)) < tol

fix_rownames <- function(df) {
  attr(df, "row.names") <- c(NA, as.integer(-nrow(df)))
  return(df)
}

#' @rdname duckdb_result-class
#' @param res Query result to be converted to an Arrow Table
#' @param chunk_size The chunk size
#' @export
duckdb_fetch_arrow <- function(res, chunk_size = 1000000) {
  if (chunk_size <= 0) {
    stop("Chunk Size must be higher than 0")
  }
  rapi_execute_arrow(res@query_result, chunk_size)
}

#' @rdname duckdb_result-class
#' @param res Query result to be converted to a Record Batch Reader
#' @param chunk_size The chunk size
#' @export
duckdb_fetch_record_batch <- function(res, chunk_size = 1000000) {
  if (chunk_size <= 0) {
    stop("Chunk Size must be higher than 0")
  }
  rapi_record_batch(res@query_result, chunk_size)
}

set_output_tz <- function(x, timezone, convert) {
  if (timezone == "UTC") {
    return(x)
  }

  tz_convert <- switch(convert,
    with = tz_convert,
    force = tz_force
  )

  is_datetime <- which(vapply(x, inherits, "POSIXt", FUN.VALUE = logical(1)))

  if (length(is_datetime) > 0) {
    x[is_datetime] <- lapply(x[is_datetime], tz_convert, timezone)
  }
  x
}

tz_convert <- function(x, timezone) {
  attr(x, "tzone") <- timezone
  x
}

tz_force <- function(x, timezone) {
  # convert to character, stripping the timezone
  ct <- as.character(x, usetz = FALSE)
  # recreate the POSIXct with specified timezone
  as.POSIXct(ct, tz = timezone)
}
