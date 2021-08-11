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

  res <- new("duckdb_result", connection = connection, stmt_lst = stmt_lst, env = env, arrow=arrow)

  if (stmt_lst$n_param == 0) {
    if (arrow){
      query_result <- duckdb_execute(res)
      new_res <- new("duckdb_result", connection = connection, stmt_lst = stmt_lst, env = env, arrow=arrow, query_result=query_result)
      return (new_res)
    }
    else{
      duckdb_execute(res)
    }

  }


  return(res)
}

duckdb_execute <- function(res) {
  out <- .Call(duckdb_execute_R, res@stmt_lst$ref, res@arrow)
  duckdb_post_execute(res, out)
}

duckdb_post_execute <- function(res, out) {
  if (!res@arrow) {
    out <- list_to_df(out)

    if (res@stmt_lst$type != "SELECT") {
      res@env$rows_affected <- sum(as.numeric(out[[1]]))
    }

    res@env$resultset <- out
  }

  out
}

list_to_df <- function(x) {
  attr(x, "row.names") <- c(NA_integer_, -length(x[[1]]))
  class(x) <- "data.frame"
  x
}


#' @rdname duckdb_result-class
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "duckdb_result",
  function(object) {
    message(sprintf("<duckdb_result %s connection=%s statement='%s'>", extptr_str(object@stmt_lst$ref), extptr_str(object@connection@conn_ref), object@stmt_lst$str))
    invisible(NULL)
  }
)

#' @rdname duckdb_result-class
#' @inheritParams DBI::dbClearResult
#' @export
setMethod(
  "dbClearResult", "duckdb_result",
  function(res, ...) {
    if (res@env$open) {
      .Call(duckdb_release_R, res@stmt_lst$ref)
      res@env$open <- FALSE
    } else {
      warning("Result was cleared already")
    }
    return(invisible(TRUE))
  }
)

# as per is.integer documentation
is_wholenumber <- function(x, tol = .Machine$double.eps^0.5) abs(x - round(x)) < tol

fix_rownames <- function(df) {
  attr(df, "row.names") <- c(NA, as.integer(-nrow(df)))
  return(df)
}

#' @rdname duckdb_result-class
#' @param res Query result to be converted to an Arrow Table
#' @param stream If we are streaming the query result or returning it all at once
#' @param vector_per_chunk If streaming, how many vectors per chunk we should emit
#' @param return_table If we return results as a list of RecordBatches or an Arrow Table
#' @export
duckdb_fetch_arrow <- function(res,stream=FALSE,vector_per_chunk=1,return_table=FALSE) {
  if (vector_per_chunk < 0) {
      stop("cannot fetch negative vector_per_chunk")
  }
  result <- .Call(duckdb_fetch_arrow_R, res@query_result,stream,vector_per_chunk,return_table)
  return (result)
}

#' @rdname duckdb_result-class
#' @param res Query result to be converted to an Arrow Table
#' @export
duckdb_fetch_record_batch <- function(res) {
  result <- .Call(duckdb_fetch_record_batch_R, res@query_result)
  return (result)
}



#' @rdname duckdb_result-class
#' @inheritParams DBI::dbFetch
#' @importFrom utils head
#' @export
setMethod(
  "dbFetch", "duckdb_result",
  function(res, n = -1, ...) {
    if (!res@env$open) {
      stop("result set was closed")
    }
    if (is.null(res@env$resultset)) {
      stop("Need to call `dbBind()` before `dbFetch()`")
    }

    if (length(n) != 1) {
      stop("need exactly one value in n")
    }
    if (is.infinite(n)) {
      n <- -1
    }
    if (n < -1) {
      stop("cannot fetch negative n other than -1")
    }
    if (!is_wholenumber(n)) {
      stop("n needs to be not a whole number")
    }
    if (res@stmt_lst$type != "SELECT") {
      warning("Should not call dbFetch() on results that do not come from SELECT")
      return(data.frame())
    }

    if (res@arrow) {
        stop("Cannot dbFetch() an Arrow result")
    }

    timezone_out <- res@connection@timezone_out
    tz_out_convert <- res@connection@tz_out_convert

    # FIXME this is ugly
    if (n == 0) {
      return(utils::head(res@env$resultset, 0))
    }
    if (res@env$rows_fetched < 0) {
      res@env$rows_fetched <- 0
    }
    if (res@env$rows_fetched >= nrow(res@env$resultset)) {
      df <- fix_rownames(res@env$resultset[F, , drop = F])
      df <- set_output_tz(df, timezone_out, tz_out_convert)
      return(df)
    }
    # special case, return everything
    if (n == -1 && res@env$rows_fetched == 0) {
      res@env$rows_fetched <- nrow(res@env$resultset)
      df <- res@env$resultset
      df <- set_output_tz(df, timezone_out, tz_out_convert)
      return(df)
    }
    if (n > -1) {
      n <- min(n, nrow(res@env$resultset) - res@env$rows_fetched)
      res@env$rows_fetched <- res@env$rows_fetched + n
      df <- res@env$resultset[(res@env$rows_fetched - n + 1):(res@env$rows_fetched), , drop = F]
      df <- set_output_tz(df, timezone_out, tz_out_convert)
      return(fix_rownames(df))
    }
    start <- res@env$rows_fetched + 1
    res@env$rows_fetched <- nrow(res@env$resultset)
    df <- res@env$resultset[nrow(res@env$resultset), , drop = F]

    df <- set_output_tz(df, timezone_out, tz_out_convert)
    return(fix_rownames(df))
  }
)

#' @rdname duckdb_result-class
#' @inheritParams DBI::dbHasCompleted
#' @export
setMethod(
  "dbHasCompleted", "duckdb_result",
  function(res, ...) {
    if (!res@env$open) {
      stop("result has already been cleared")
    }

    if (is.null(res@env$resultset)) {
      FALSE
    } else if (res@stmt_lst$type == "SELECT") {
      res@env$rows_fetched == nrow(res@env$resultset)
    } else {
      TRUE
    }
  }
)

#' @rdname duckdb_result-class
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "duckdb_result",
  function(dbObj, ...) {
    # Optional
    getMethod("dbGetInfo", "DBIResult", asNamespace("DBI"))(dbObj, ...)
  }
)

#' @rdname duckdb_result-class
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "duckdb_result",
  function(dbObj, ...) {
    return(dbObj@env$open)
  }
)

#' @rdname duckdb_result-class
#' @inheritParams DBI::dbGetStatement
#' @export
setMethod(
  "dbGetStatement", "duckdb_result",
  function(res, ...) {
    if (!res@env$open) {
      stop("result has already been cleared")
    }
    return(res@stmt_lst$str)
  }
)

#' @rdname duckdb_result-class
#' @inheritParams DBI::dbColumnInfo
#' @export
setMethod(
  "dbColumnInfo", "duckdb_result",
  function(res, ...) {
    if (!res@env$open) {
      stop("result has already been cleared")
    }
    return(data.frame(name = res@stmt_lst$names, type = res@stmt_lst$rtypes, stringsAsFactors = FALSE))
  }
)

#' @rdname duckdb_result-class
#' @inheritParams DBI::dbGetRowCount
#' @export
setMethod(
  "dbGetRowCount", "duckdb_result",
  function(res, ...) {
    if (!res@env$open) {
      stop("result has already been cleared")
    }
    return(res@env$rows_fetched)
  }
)

#' @rdname duckdb_result-class
#' @inheritParams DBI::dbGetRowsAffected
#' @export
setMethod(
  "dbGetRowsAffected", "duckdb_result",
  function(res, ...) {
    if (!res@env$open) {
      stop("result has already been cleared")
    }
    if (is.null(res@env$resultset)) {
      return(NA_integer_)
    }
    return(res@env$rows_affected)
  }
)

#' @rdname duckdb_result-class
#' @inheritParams DBI::dbBind
#' @export
setMethod(
  "dbBind", "duckdb_result",
  function(res, params, ...) {
    if (!res@env$open) {
      stop("result has already been cleared")
    }
    res@env$rows_fetched <- 0
    res@env$resultset <- data.frame()

    params <- as.list(params)
    if (!is.null(names(params))) {
      stop("`params` must not be named")
    }
    out <- .Call(duckdb_bind_R, res@stmt_lst$ref, params, res@arrow)
    if (length(out) == 1) {
      out <- out[[1]]
    } else {
      out <- do.call(rbind, lapply(out, list_to_df))
    }
    duckdb_post_execute(res, out)
    invisible(res)
  }
)

set_output_tz <- function(x, timezone, convert) {
  if (timezone == "UTC") return(x)

  tz_convert <- switch(convert,
                       with = tz_convert,
                       force = tz_force)

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
