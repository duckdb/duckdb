#' @rdname duckdb_driver-class
#' @usage NULL
dbDataType__duckdb_driver <- function(dbObj, obj, ...) {
  # FIXME: Use RApiTypes::DetectRType()
  if (is.null(obj)) stop("NULL parameter")
  if (is.data.frame(obj)) {
    return(vapply(obj, function(x) dbDataType(dbObj, x), FUN.VALUE = "character"))
  }
  #  else if (int64 && inherits(obj, "integer64")) "BIGINT"
  else if (inherits(obj, "Date")) {
    "DATE"
  } else if (inherits(obj, "difftime")) {
    "TIME"
  } else if (is.logical(obj)) {
    "BOOLEAN"
  } else if (is.integer(obj)) {
    "INTEGER"
  } else if (is.numeric(obj)) {
    "DOUBLE"
  } else if (inherits(obj, "POSIXt")) {
    "TIMESTAMP"
  } else if (inherits(obj, "blob") || (is.list(obj) && all(vapply(obj, typeof, FUN.VALUE = "character") %in% c("raw", "NULL")))) {
    "BLOB"
  } else {
    "STRING"
  }
}

#' @rdname duckdb_driver-class
#' @export
setMethod("dbDataType", "duckdb_driver", dbDataType__duckdb_driver)
