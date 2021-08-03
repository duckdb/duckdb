#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "odbc_fetch.hpp"

SQLRETURN SQLGetData(SQLHSTMT statement_handle, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                     SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {

	return duckdb::GetDataStmtResult(statement_handle, col_or_param_num, target_type, target_value_ptr, buffer_length,
	                                 str_len_or_ind_ptr);
}

SQLRETURN SQLFetch(SQLHSTMT statement_handle) {
	return duckdb::FetchStmtResult(statement_handle);
}

SQLRETURN SQLFetchScroll(SQLHSTMT statement_handle, SQLSMALLINT fetch_orientation, SQLLEN fetch_offset) {
	switch (fetch_orientation) {
	case SQL_FETCH_ABSOLUTE:
	case SQL_FETCH_PRIOR:
	case SQL_FETCH_NEXT:
		// passing "fetch_offset - 1" because DuckDB rowset start from 0 instead of row 1
		return duckdb::FetchStmtResult(statement_handle, fetch_orientation, fetch_offset - 1);
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLRowCount(SQLHSTMT statement_handle, SQLLEN *row_count_ptr) {
	return duckdb::WithStatementResult(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (!row_count_ptr) {
			return SQL_ERROR;
		}
		// TODO row_count isn't work well yet, left to fix latter
		*row_count_ptr = stmt->odbc_fetcher->row_count;

		// *row_count_ptr = -1; // we don't actually know most of the time
		return SQL_SUCCESS;
	});
}
