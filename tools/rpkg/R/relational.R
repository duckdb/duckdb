# these wrappers are a bit annoying, maybe there's a way around. Kirill?

# expressions

#' Create a column reference expression
#' @param name the column name to be referenced
#' @param table the optional table name or a relation object to be referenced
#' @return a column reference expression
#' @export
#' @examples
#' col_ref_expr <- expr_reference("some_column_name")
#' col_ref_expr2 <- expr_reference("some_column_name", "some_table_name")
expr_reference <- function(name, table = "") {
    if (inherits(table, "duckdb_relation")) {
        table <- rel_alias(table)
    }
    rapi_expr_reference(name, table)
}

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

#' Set the alias for an expression
#' @param expr the expression
#' @param alias the alias
#' @export
#' @examples
#' expr_set_alias(expr_constant(42), "my_alias")
expr_set_alias <- rapi_expr_set_alias


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

#' @export
head.duckdb_relation <- function(x, n=6L, ...) {
    rapi_rel_limit(x, n)
}

#' Lazily project a DuckDB relation object
#' @param rel the DuckDB relation object
#' @param exprs a list of DuckDB expressions to project
#' @return the now projected `duckdb_relation` object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel2 <- rel_project(rel, list(expr_reference("cyl"), expr_reference("disp")))
rel_project <- rapi_rel_project

#' Lazily filter a DuckDB relation object
#' @param rel the DuckDB relation object
#' @param exprs a list of DuckDB expressions to filter by
#' @return the now filtered `duckdb_relation` object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb())
#' DBI::dbExecute(con, 'CREATE MACRO gt(a, b) AS a > b')
#' rel <- rel_from_df(con, mtcars)
#' rel2 <- rel_filter(rel, list(expr_function("gt", list(expr_reference("cyl"), expr_constant("6")))))
rel_filter <- rapi_rel_filter

#' Lazily aggregate a DuckDB relation object
#' @param rel the DuckDB relation object
#' @param groups a list of DuckDB expressions to group by
#' @param aggregates a (optionally named) list of DuckDB expressions with aggregates to compute
#' @return the now aggregated `duckdb_relation` object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb())
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
#' con <- DBI::dbConnect(duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel2 <- rel_order(rel, list(expr_reference("hp")))
rel_order <- rapi_rel_order

#' Lazily INNER join two DuckDB relation objects
#' @param left the left-hand-side DuckDB relation object
#' @param right the right-hand-side DuckDB relation object
#' @param conds a list of DuckDB expressions to use for the join
#' @return a new `duckdb_relation` object resulting from the join
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb())
#' DBI::dbExecute(con, 'CREATE MACRO eq(a, b) AS a = b')
#' left <- rel_from_df(con, mtcars)
#' right <- rel_from_df(con, mtcars)
#' cond <- list(expr_function("eq", list(expr_reference("cyl", left), expr_reference("cyl", right))))
#' rel2 <- rel_inner_join(left, right, cond)
rel_inner_join <- rapi_rel_inner_join

#' Lazily compute a distinct result on a DuckDB relation object
#' @param rel the input DuckDB relation object
#' @return a new `duckdb_relation` object with distinct rows
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel2 <- rel_distinct(rel)
rel_distinct <- rapi_rel_distinct

#' Run a SQL query on a DuckDB relation object
#' @param rel the DuckDB relation object
#' @param sql a SQL query to run, use `_` to refer back to the relation
#' @return the now aggregated `duckdb_relation` object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel2 <- rel_sql(rel, "SELECT hp, cyl FROM _ WHERE hp > 100")
rel_sql <- rapi_rel_sql

#' Print the EXPLAIN output for a DuckDB relation object
#' @param rel the DuckDB relation object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel_explain(rel)
rel_explain <- function(rel) {
    cat(rapi_rel_explain(rel)[[2]][[1]])
    invisible(NULL)
}

#' Get the internal alias for a DuckDB relation object
#' @param rel the DuckDB relation object
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel_alias(rel)
rel_alias <- rapi_rel_alias

#' Set the internal alias for a DuckDB relation object
#' @param rel the DuckDB relation object
#' @param alias the new alias
#' @export
#' @examples
#' con <- DBI::dbConnect(duckdb())
#' rel <- rel_from_df(con, mtcars)
#' rel_set_alias(rel, "my_new_alias")
rel_set_alias <- rapi_rel_set_alias
