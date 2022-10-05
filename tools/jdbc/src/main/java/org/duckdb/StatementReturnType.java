package org.duckdb;

public enum StatementReturnType {
    QUERY_RESULT, // the statement returns a query result (e.g. for display to the user)
    CHANGED_ROWS, // the statement returns a single row containing the number of changed rows (e.g. an insert stmt)
    NOTHING       // the statement returns nothing
}
