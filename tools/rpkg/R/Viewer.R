# Functions used to connect to Connections Pane in Rstudio
# Implementing connections contract: https://rstudio.github.io/rstudio-extensions/connections-contract.html
duckdb_ListObjectTypes <- function(con) {
  object_types <- list(table = list(contains="data"))

  types <- dbGetQuery(con, "SELECT DISTINCT type FROM sqlite_master")[[1]]
  if (any(types =="view")){
    object_types <- c(object_types, view=list(contains="data"))
  }
  object_types
}

duckdb_ListObjects <- function(con, catalog = NULL, schema = NULL, name = NULL, type = NULL, ...) {
  objects <- dbGetQuery(con, "SELECT name,type FROM sqlite_master")
  objects <- objects[objects$type %in% c("table","view"),]
  objects
}

duckdb_ListColumns <- function(con, table = NULL, view = NULL,
                                           catalog = NULL, schema = NULL, ...) {
  if (is.null(table)){
    table <- view
  }

  tb <- dbGetQuery(
    con,
    paste("SELECT * FROM",dbQuoteIdentifier(con, table),"WHERE FALSE")
  )

  name <- names(tb)
  type <- sapply(tb, class)

  data.frame(
    name = name,
    type = type,
    stringsAsFactors = FALSE
  )
}

duckdb_PreviewObject <- function(con, rowLimit, table = NULL, view = NULL, ...) {
  # extract object name from arguments
  name <- if (is.null(table)) view else table
  dbGetQuery(con, paste("SELECT * FROM", dbQuoteIdentifier(con, name)), n = rowLimit)
}

duckdb_ConnectionIcon <- function(con) {
  system.file("icons/duckdb.png", package="duckdb")
}

duckdb_ConnectionActions <- function(con) {
  actions <- list()
  actions <- c(actions, list(
    Help = list(
      icon = "",
      callback = function() {
        utils::browseURL("https://duckdb.org/docs/api/r")
      }
    )
  ))

  actions
}

on_connection_closed <- function(con) {
  # make sure we have an observer
  observer <- getOption("connectionObserver")
  if (is.null(observer))
    return(invisible(NULL))

  host <- get_host(con)
  observer$connectionClosed("duckdb", host)
}

get_host <- function(con){
  con@driver@dbdir
}

on_connection_updated <- function(con, hint) {
  observer <- getOption("connectionObserver")
  if (is.null(observer))
    return(invisible(NULL))
  host <- get_host(con)
  observer$connectionUpdated("duckdb", host, hint = hint)
}

on_connection_opened <- function(con) {
  code <- paste0(
"library(duckdb)
drv <- duckdb(\"", con@driver@dbdir,"\", read_only = ", con@driver@read_only ,")
con <- dbConnect(drv)
")
  observer <- getOption("connectionObserver")
  if (is.null(observer))
    return(invisible(NULL))

  icon <- duckdb_ConnectionIcon(con)

  host <- get_host(con)

    # let observer know that connection has opened
  observer$connectionOpened(
    # connection type
    type = "duckdb",

    # name displayed in connection pane (to be improved)
    displayName = paste0("DuckDB "
                        , '"' , host, '"'
                        , if (con@driver@read_only) " (readonly)"
                        ),

    host = host,

    icon = icon,

    # connection code
    connectCode = code,

    # disconnection code
    disconnect = function() {
      dbDisconnect(con, shutdown = TRUE)
    },

    listObjectTypes = function () {
      duckdb_ListObjectTypes(con)
    },

    # table enumeration code
    listObjects = function(...) {
      duckdb_ListObjects(con, ...)
    },

    # column enumeration code
    listColumns = function(...) {
      duckdb_ListColumns(con, ...)
    },

    # table preview code
    previewObject = function(rowLimit, ...) {
      duckdb_PreviewObject(con, rowLimit, ...)
    },

    # other actions that can be executed on this connection
    actions = duckdb_ConnectionActions(con),

    # raw connection object
    connectionObject = con
  )
}
