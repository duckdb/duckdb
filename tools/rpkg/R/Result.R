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
    arrow = "logical"
  )
)

duckdb_result <- function(connection, stmt_lst, arrow) {
  env <- new.env(parent = emptyenv())
  env$rows_fetched <- 0
  env$open <- TRUE
  env$rows_affected <- 0

  res <- new("duckdb_result", connection = connection, stmt_lst = stmt_lst, env = env, arrow=arrow)

  if (stmt_lst$n_param == 0) {
    duckdb_execute(res)
  }

  return(res)
}

duckdb_execute <- function(res) {
  res@env$resultset <- .Call(duckdb_execute_R, res@stmt_lst$ref, res@arrow)
  if (!res@arrow) {
      attr(res@env$resultset, "row.names") <-
        c(NA_integer_, as.integer(-1 * length(res@env$resultset[[1]])))
      class(res@env$resultset) <- "data.frame"
  }
  if (res@stmt_lst$type != "SELECT") {
    res@env$rows_affected <- as.numeric(res@env$resultset[[1]][1])
  }
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

#' @export
duckdb_fetch_arrow <- function(res) {
  return (res@env$resultset)
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
    if (res@stmt_lst$type != "SELECT") {
      return(TRUE)
    }
    return(res@env$rows_fetched == nrow(res@env$resultset))
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

    invisible(.Call(duckdb_bind_R, res@stmt_lst$ref, params))
    duckdb_execute(res)
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
