#' @include Connection.R
NULL

duckdb_result <- function(connection, stmt_lst) {
  env <- new.env(parent=emptyenv())
  env$rows_fetched <- 0
  env$open <- TRUE
  env$rows_affected <- 0

  res <- new("duckdb_result", connection = connection, stmt_lst = stmt_lst, env=env)

  if (stmt_lst$n_param == 0) {
    duckdb_execute(res)
  }

  return(res)
}

duckdb_execute <- function(res) {
  res@env$resultset <- .Call(duckdb_execute_R, res@stmt_lst$ref)
  attr(res@env$resultset, "row.names") <-
          c(NA_integer_, as.integer(-1 * length(res@env$resultset[[1]])))
  class(res@env$resultset) <- "data.frame"
  if (res@stmt_lst$type != 'SELECT') {
    res@env$rows_affected <- as.numeric(res@env$resultset[[1]][1])
  }
}

#' @rdname DBI
#' @export
setClass(
  "duckdb_result",
  contains = "DBIResult",
  slots = list(
    connection = "duckdb_connection",
    stmt_lst = "list",
    env = "environment"
  )
)

#' @rdname DBI
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "duckdb_result",
  function(object) {
    cat(sprintf("<duckdb_result %s connection=%s statement='%s'>\n", extptr_str(object@stmt_lst$ref), extptr_str(object@connection@conn_ref), object@stmt_lst$str))
  })

#' @rdname DBI
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
  })

# as per is.integer documentation
is_wholenumber <- function(x, tol = .Machine$double.eps^0.5)  abs(x - round(x)) < tol

fix_rownames <- function(df) {
  attr(df, "row.names") <- c(NA, as.integer(-nrow(df)))
  return(df)
}


#' @rdname DBI
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

# FIXME this is ugly
    if (n == 0) {
      return(utils::head(res@env$resultset, 0))
    }
    if (res@env$rows_fetched < 0) {
      res@env$rows_fetched <- 0
    }
    if (res@env$rows_fetched >= nrow(res@env$resultset)) {
      return(fix_rownames(res@env$resultset[F,, drop=F]))
    }
    # special case, return everything
    if (n == -1 && res@env$rows_fetched == 0) {
      res@env$rows_fetched <- nrow(res@env$resultset)
      return(res@env$resultset)
    }
    if (n > -1) {
      n <- min(n, nrow(res@env$resultset) - res@env$rows_fetched)
      res@env$rows_fetched <- res@env$rows_fetched + n
      df <- res@env$resultset[(res@env$rows_fetched - n + 1):(res@env$rows_fetched),, drop=F]
      return(fix_rownames(df))
    }
    start <- res@env$rows_fetched + 1
    res@env$rows_fetched <- nrow(res@env$resultset)
    df <- res@env$resultset[nrow(res@env$resultset),, drop=F]
    return(fix_rownames(df))
  })

#' @rdname DBI
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
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "duckdb_result",
  function(dbObj, ...) {
    # Optional
    getMethod("dbGetInfo", "DBIResult", asNamespace("DBI"))(dbObj, ...)
  })

#' @rdname DBI
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "duckdb_result",
  function(dbObj, ...) {
    return(dbObj@env$open)
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetStatement
#' @export
setMethod(
  "dbGetStatement", "duckdb_result",
  function(res, ...) {
    if (!res@env$open) {
      stop("result has already been cleared")
    }
    return(res@stmt_lst$str)
  })

#' @rdname DBI
#' @inheritParams DBI::dbColumnInfo
#' @export
setMethod(
  "dbColumnInfo", "duckdb_result",
  function(res, ...) {
    return(data.frame(name=res@stmt_lst$names, type=res@stmt_lst$rtypes, stringsAsFactors=FALSE))

  })

#' @rdname DBI
#' @inheritParams DBI::dbGetRowCount
#' @export
setMethod(
  "dbGetRowCount", "duckdb_result",
  function(res, ...) {
    if (!res@env$open) {
      stop("result has already been cleared")
    }
   return(res@env$rows_fetched)
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetRowsAffected
#' @export
setMethod(
  "dbGetRowsAffected", "duckdb_result",
  function(res, ...) {
     if (!res@env$open) {
      stop("result has already been cleared")
    }
  return (res@env$rows_affected)
  })

#' @rdname DBI
#' @inheritParams DBI::dbBind
#' @importFrom testthat skip
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
  })
