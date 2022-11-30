# adapted from sort-of reference at https://github.com/r-dbi/odbc/blob/main/R/Viewer.R
rs_list_object_types <- function(connection) {
  # slurp all the objects in the database so we can determine the correct
  # object hierarchy

  # all databases contain tables, at a minimum or so someone claims
  obj_types <- list(table = list(contains = "data"))

  # see if we have views too
  if (dbGetQuery(connection, "SELECT 'VIEW' IN (SELECT DISTINCT table_type FROM information_schema.tables) s")[[1]]) {
    obj_types <- c(obj_types, list(view = list(contains = "data")))
  }

  # check for multiple schema or a named schema
  if (dbGetQuery(connection, "SELECT COUNT(DISTINCT table_schema) > 0 FROM information_schema.tables")[[1]]) {
    obj_types <- list(schema = list(contains = obj_types))
  }

  obj_types
}

rs_list_objects <- function(connection, catalog = NULL, schema = NULL, name = NULL, type = NULL, ...) {
  # if no schema was supplied but this database has schema, return a list of
  # schema
  if (is.null(schema)) {
    dbGetQuery(connection, "SELECT DISTINCT table_schema \"name\", 'schema' \"type\" FROM information_schema.tables ORDER BY table_schema")
  }

  if (is.null(schema)) {
    schema <- NA
  }

  if (is.null(name)) {
    name <- NA
  }

  if (is.null(type)) {
    type <- "%"
  } else {
    type <- paste0("%", type)
  }
  # behold
  dbGetQuery(
    connection, '
    SELECT table_name "name",
      CASE WHEN table_type LIKE \'%TABLE\' THEN \'table\' ELSE LOWER(table_type) END "type"
    FROM information_schema.tables
    WHERE (?::STRING IS NULL OR table_schema = ?) AND
      (?::STRING IS NULL OR table_name = ?) AND
      table_type ILIKE ?
    ORDER BY table_schema, table_type, table_name',
    list(schema, schema, name, name, type)
  )
}

rs_list_columns <- function(connection, table, catalog = NULL, schema = NULL, ...) {
  if (is.null(schema)) {
    schema <- NA
  }
  dbGetQuery(
    connection, "
    SELECT column_name \"name\", data_type \"field.type\"
    FROM information_schema.columns
    WHERE (?::STRING IS NULL OR table_schema = ?) AND
      table_name = ?
    ORDER BY ordinal_position",
    list(schema, schema, table)
  )
}

rs_preview <- function(connection, rowLimit, table = NULL, view = NULL, schema = NULL, ...) {
  # Error if both table and view are passed
  if (!is.null(table) && !is.null(view)) {
    stop("`table` and `view` can not both be used", call. = FALSE)
  }

  # Error if neither table and view are passed
  if (is.null(table) && is.null(view)) {
    stop("`table` and `view` can not both be `NULL`", call. = FALSE)
  }

  name <- if (!is.null(table)) {
    table
  } else {
    view
  }

  # append schema if specified
  if (!is.null(schema)) {
    name <- paste(dbQuoteIdentifier(connection, schema), dbQuoteIdentifier(connection, name), sep = ".")
  }

  dbGetQuery(connection, paste("SELECT * FROM ", name, " LIMIT ?"), list(rowLimit))
}

rs_actions <- function(connection) {
  list(
    Help = list(
      # show README for this package as the help; we will update to a more
      # helpful (and/or more driver-specific) website once one exists
      icon = "",
      callback = function() {
        utils::browseURL("https://duckdb.org/docs/api/r")
      }
    )
  )
}

rs_check_disabled <- function(observer) {
  if (getOption("duckdb.force_rstudio_connection_pane", FALSE)) {
    return(FALSE)
  }
  is.null(observer) || !interactive() || !getOption("duckdb.enable_rstudio_connection_pane", FALSE)
}

rs_on_connection_closed <- function(connection) {
  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (rs_check_disabled(observer)) {
    return(invisible(NULL))
  }

  type <- "DuckDB"
  host <- connection@driver@dbdir
  observer$connectionClosed(type, host)
}

rs_on_connection_updated <- function(connection, hint) {
  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (rs_check_disabled(observer)) {
    return(invisible(NULL))
  }

  type <- "DuckDB"
  host <- connection@driver@dbdir
  observer$connectionUpdated(type, host, hint = hint)
}

rs_on_connection_opened <- function(connection, code = "") {
  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (rs_check_disabled(observer)) {
    return(invisible(NULL))
  }

  # use the database name as the display name
  display_name <- "DuckDB"
  server_name <- connection@driver@dbdir

  # append the server name if we know it, and it isn't the same as the database name
  # (this can happen for serverless, nameless databases such as SQLite)
  if (!is.null(server_name) && nzchar(server_name) &&
    !identical(server_name, display_name)) {
    display_name <- paste(display_name, "-", server_name)
  }

  # let observer know that connection has opened
  observer$connectionOpened(
    # connection type
    type = "DuckDB",

    # name displayed in connection pane
    displayName = display_name,

    # host key
    host = connection@driver@dbdir,

    # icon for connection
    icon = system.file("icons/duckdb.png", package = "duckdb"),

    # connection code
    connectCode = code,

    # disconnection code
    disconnect = function() {
      dbDisconnect(connection)
    },
    listObjectTypes = function() {
      rs_list_object_types(connection)
    },

    # table enumeration code
    listObjects = function(...) {
      rs_list_objects(connection, ...)
    },

    # column enumeration code
    listColumns = function(...) {
      rs_list_columns(connection, ...)
    },

    # table preview code
    previewObject = function(rowLimit, ...) {
      rs_preview(connection, rowLimit, ...)
    },

    # other actions that can be executed on this connection
    actions = rs_actions(connection),

    # raw connection object
    connectionObject = connection
  )
}
