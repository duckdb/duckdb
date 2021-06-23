#include "duckdb_odbc.hpp"

SQLRETURN SQLGetData(SQLHSTMT statement_handle, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                     SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {

	return duckdb::WithStatementResult(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (!target_value_ptr) {
			return SQL_ERROR;
		}

		if (!stmt->chunk) {
			return SQL_ERROR;
		}
		auto val = stmt->chunk->GetValue(col_or_param_num - 1, stmt->chunk_row);
		if (val.is_null) {
			if (!str_len_or_ind_ptr) {
				return SQL_ERROR;
			}
			*str_len_or_ind_ptr = SQL_NULL_DATA;
			return SQL_SUCCESS;
		}

		switch (target_type) {
		case SQL_C_SLONG:
			D_ASSERT(((size_t)buffer_length) >= sizeof(int));
			duckdb::Store<int>(val.GetValue<int>(), (duckdb::data_ptr_t)target_value_ptr);
			return SQL_SUCCESS;

		case SQL_CHAR: {
			auto out_len = snprintf((char *)target_value_ptr, buffer_length, "%s", val.GetValue<std::string>().c_str());

			if (str_len_or_ind_ptr) {
				*str_len_or_ind_ptr = out_len;
			}
			return SQL_SUCCESS;
		}
			// TODO other types
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLFetch(SQLHSTMT statement_handle) {
	return duckdb::WithStatementResult(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (!stmt->open) {
			return SQL_NO_DATA;
		}
		if (!stmt->chunk || ((duckdb::idx_t)stmt->chunk_row) >= stmt->chunk->size() - 1) {
			// TODO try /catch
			try {
				stmt->chunk = stmt->res->Fetch();
			} catch (duckdb::Exception &e) {
				// TODO this is quite dirty, we should have separate error holder
				stmt->res->error = e.what();
				stmt->res->success = false;
				stmt->open = false;
				return SQL_ERROR;
			}
			if (!stmt->chunk) {
				stmt->open = false;
				return SQL_NO_DATA;
			}
			stmt->chunk_row = -1;
		}
		if (stmt->rows_fetched_ptr) {
			(*stmt->rows_fetched_ptr)++;
		}
		stmt->chunk_row++;

		// now fill buffers in fetch if set
		// TODO actually vectorize this
		for (duckdb::idx_t col_idx = 0; col_idx < stmt->stmt->ColumnCount(); col_idx++) {
			auto bound_buf = stmt->bound_cols[col_idx];
			if (bound_buf.IsBound()) {
				if (!SQL_SUCCEEDED(SQLGetData(statement_handle, col_idx + 1, bound_buf.type, bound_buf.ptr,
				                              bound_buf.len, bound_buf.strlen_or_ind))) {
					return SQL_ERROR;
				}
			}
		}

		D_ASSERT(stmt->chunk);
		D_ASSERT(((size_t)stmt->chunk_row) < stmt->chunk->size());
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
		*row_count_ptr = -1; // we don't actually know most of the time
		return SQL_SUCCESS;
	});
}