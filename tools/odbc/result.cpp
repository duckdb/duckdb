#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "odbc_fetch.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"

#include "duckdb/main/prepared_statement_data.hpp"

SQLRETURN SQL_API SQLGetData(SQLHSTMT statement_handle, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                             SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {

	return duckdb::GetDataStmtResult(statement_handle, col_or_param_num, target_type, target_value_ptr, buffer_length,
	                                 str_len_or_ind_ptr);
}

static SQLRETURN ExecuteBeforeFetch(SQLHSTMT statement_handle) {
	return duckdb::WithStatementPrepared(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		// case there is a result set, just fetch from it
		if (stmt->res && stmt->res->success) {
			return SQL_SUCCESS;
		}
		// check if it's needed to execute the stmt before fetch
		if (stmt->param_desc->HasParamSetToProcess()) {
			auto rc = duckdb::SingleExecuteStmt(stmt);
			if (rc == SQL_SUCCESS || rc == SQL_STILL_EXECUTING) {
				return SQL_SUCCESS;
			}
			return rc;
		}
		return SQL_SUCCESS;
	});
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT statement_handle) {
	auto ret = duckdb::WithStatement(
	    statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN { return stmt->odbc_fetcher->DummyFetch(); });
	if (ret != SQL_NEED_DATA) {
		return ret;
	}

	ret = ExecuteBeforeFetch(statement_handle);
	if (ret != SQL_SUCCESS) {
		return ret;
	}
	return duckdb::FetchStmtResult(statement_handle);
}

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT statement_handle, SQLSMALLINT fetch_orientation, SQLLEN fetch_offset) {
	switch (fetch_orientation) {
	case SQL_FETCH_FIRST:
	case SQL_FETCH_ABSOLUTE:
	case SQL_FETCH_PRIOR:
	case SQL_FETCH_NEXT:
		// passing "fetch_offset - 1", the DuckDB's internal row index starts in 0
		return duckdb::FetchStmtResult(statement_handle, fetch_orientation, fetch_offset - 1);
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQL_API SQLRowCount(SQLHSTMT statement_handle, SQLLEN *row_count_ptr) {
	return duckdb::WithStatementResult(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (!row_count_ptr) {
			return SQL_ERROR;
		}
		// TODO row_count isn't work well yet, left to fix latter
		*row_count_ptr = stmt->odbc_fetcher->GetRowCount();

		switch (stmt->stmt->data->statement_type) {
		case duckdb::StatementType::INSERT_STATEMENT:
		case duckdb::StatementType::UPDATE_STATEMENT:
		case duckdb::StatementType::DELETE_STATEMENT:
			break;
		default:
			*row_count_ptr = -1;
		}

		// *row_count_ptr = -1; // we don't actually know most of the time
		return SQL_SUCCESS;
	});
}
