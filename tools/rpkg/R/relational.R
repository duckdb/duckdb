# these wrappers are a bit annoying, maybe there's a way around. Kirill?

# expressions

#' Create a column reference expression
#' @param ref the column name to be referenced
#' @return a column reference expression
#' @export
#' @examples
#' col_ref_expr <- expr_reference("some_column_name")
expr_reference <- rapi_expr_reference

#' Create a constant expression
#' @param val the constant value
#' @return a constant expression
#' @export
#' @examples
#' const_int_expr <- expr_constant(42)
#' const_str_expr <- expr_constant("Hello, World")
expr_constant <- rapi_expr_constant

#' Create a function call expression
#' @param name the function name
#' @param args the a list of expressions for the function arguments
#' @return a function call expression
#' @export
#' @examples
#' call_expr <- expr_function("ABS", list(expr_constant(-42)))
expr_function <- rapi_expr_function

#' Convert an expression to a string for debugging purposes
#' @param expr the expression
#' @return a string representation of the expression
#' @export
#' @examples
#' expr_str <- expr_tostring(expr_constant(42))
expr_tostring <- rapi_expr_tostring

#' @export
print.duckdb_expr <- function(x, ...) {
    message("DuckDB Expression: ", duckdb::expr_tostring(x))
    invisible(NULL)
}

# relations

#' Convert a R data.frame to a DuckDB relation object
#' @param con a DuckDB DBI connection object
#' @param df the data.frame
#' @return the `duckdb_relation` object wrapping the data.frame
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb::duckdb())
#' rel <- rel_from_df(con, mtcars)
rel_from_df <- function(con, df) {
    rapi_rel_from_df(con@conn_ref, as.data.frame(df))
}

#' @export
print.duckdb_relation <- function(x, ...) {
    message("DuckDB Relation: \n", rapi_rel_tostring(x))
}

#' @export
as.data.frame.duckdb_relation <- function(x, row.names=NULL, optional=NULL, ...) {
    if (!missing(row.names) || !missing(optional)) {
        stop("row.names and optional parameters not supported")
    }
    rapi_rel_to_df(x)
}

#' @export
names.duckdb_relation <- function(x) {
    rapi_rel_names(x)
}

#' Lazily project a DuckDB relation object
#' @param rel the DuckDB relation object
#' @param exprs a list of DuckDB expressions to project
#' @return the now projected `duckdb_relation` object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb::duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel2 <- rel_project(rel, list(expr_reference("cyl"), expr_reference("disp")))
rel_project <- rapi_rel_project

#' Lazily filter a DuckDB relation object
#' @param rel the DuckDB relation object
#' @param exprs a list of DuckDB expressions to filter by
#' @return the now filtered `duckdb_relation` object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb::duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel2 <- rel_filter(rel, list(expr_function(">", list(expr_reference("cyl"), expr_constant("6")))))
rel_filter <- rapi_rel_filter

#' Lazily aggregate a DuckDB relation object
#' @param rel the DuckDB relation object
#' @param groups a list of DuckDB expressions to group by
#' @param aggregates a (optionally named) list of DuckDB expressions with aggregates to compute
#' @return the now aggregated `duckdb_relation` object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb::duckdb())
#' rel <- rel_from_df(con, mtcars)
#' aggrs <- list(avg_hp = expr_function("avg", list(expr_reference("hp"))))
#' rel2 <- rel_aggregate(rel, list(expr_reference("cyl")), aggrs)
rel_aggregate <- rapi_rel_aggregate

#' Lazily reorder a DuckDB relation object
#' @param rel the DuckDB relation object
#' @param orders a list of DuckDB expressions to order by
#' @return the now aggregated `duckdb_relation` object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb::duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel2 <- rel_order(rel, list(expr_reference("hp")))
rel_order <- rapi_rel_order

#' Run a SQL query on a DuckDB relation object
#' @param rel the DuckDB relation object
#' @param sql a SQL query to run, use `_` to refer back to the relation
#' @return the now aggregated `duckdb_relation` object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb::duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel2 <- rel_sql(rel, "SELECT hp, cyl FROM _ WHERE hp > 100")
rel_sql <- rapi_rel_sql

#' Print the EXPLAIN output for a DuckDB relation object
#' @param rel the DuckDB relation object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb::duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel_explain(rel)
rel_explain <- function(rel) {
    cat(rapi_rel_explain(rel)[[2]][[1]])
    invisible(NULL)
}

