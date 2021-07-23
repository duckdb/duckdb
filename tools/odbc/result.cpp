#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "odbc_fetch.hpp"

SQLRETURN SQLGetData(SQLHSTMT statement_handle, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                     SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {

	return duckdb::GetDataStmtResult(statement_handle, col_or_param_num, target_type, target_value_ptr, buffer_length,
	                                 str_len_or_ind_ptr);
}

SQLRETURN SQLFetch(SQLHSTMT statement_handle) {
	return duckdb::WithStatementResult(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (!stmt->open) {
			return SQL_NO_DATA;
		}
		if (!stmt->chunk || ((duckdb::idx_t)stmt->chunk_row) >= stmt->chunk->size() - 1) {
			try {
				stmt->chunk = stmt->res->Fetch();
			} catch (duckdb::Exception &e) {
				// TODO this is quite dirty, we should have separate error holder
				stmt->res->error = e.what();
				stmt->res->success = false;
				stmt->open = false;
				stmt->error_messages.emplace_back(std::string(e.what()));
				return SQL_ERROR;
			}
			if (!stmt->chunk) {
				stmt->open = false;
				return SQL_NO_DATA;
			}
			stmt->chunk_row = -1;
			stmt->row_count += stmt->chunk->size();
		}
		if (stmt->rows_fetched_ptr) {
			(*stmt->rows_fetched_ptr)++;
		}
		stmt->chunk_row++;

		if (!SQL_SUCCEEDED(stmt->odbc_fetcher->Fetch(statement_handle, stmt))) {
			return SQL_ERROR;
		}

		D_ASSERT(stmt->chunk);
		D_ASSERT(((size_t)stmt->chunk_row) <= stmt->chunk->size());
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLFetchScroll(SQLHSTMT statement_handle, SQLSMALLINT fetch_orientation, SQLLEN fetch_offset) {

	if (fetch_orientation != SQL_FETCH_NEXT) {
		return SQL_ERROR;
	}
	return SQLFetch(statement_handle);
}

SQLRETURN SQLRowCount(SQLHSTMT statement_handle, SQLLEN *row_count_ptr) {
	return duckdb::WithStatementResult(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (!row_count_ptr) {
			return SQL_ERROR;
		}
		// TODO row_count isn't work well yet, left to fix latter
		*row_count_ptr = stmt->row_count;

		// *row_count_ptr = -1; // we don't actually know most of the time
		return SQL_SUCCESS;
	});
}
