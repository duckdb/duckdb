#include "duckdb_odbc.hpp"
using namespace duckdb;

SQLRETURN SQLGetData(SQLHSTMT StatementHandle, SQLUSMALLINT Col_or_Param_Num, SQLSMALLINT TargetType,
                     SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr) {

	return WithStatementResult(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!TargetValuePtr) {
			return SQL_ERROR;
		}

		if (!stmt->chunk) {
			return SQL_ERROR;
		}
		auto val = stmt->chunk->GetValue(Col_or_Param_Num - 1, stmt->chunk_row);
		if (val.is_null) {
			if (!StrLen_or_IndPtr) {
				return SQL_ERROR;
			}
			*StrLen_or_IndPtr = SQL_NULL_DATA;
			return SQL_SUCCESS;
		}

		switch (TargetType) {
		case SQL_C_SLONG:
			D_ASSERT(BufferLength >= sizeof(int));
			Store<int>(val.GetValue<int>(), (data_ptr_t)TargetValuePtr);
			return SQL_SUCCESS;

		case SQL_CHAR: {
			auto out_len = snprintf((char *)TargetValuePtr, BufferLength, "%s", val.GetValue<string>().c_str());

			if (StrLen_or_IndPtr) {
				*StrLen_or_IndPtr = out_len;
			}
			return SQL_SUCCESS;
		}
			// TODO other types
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLFetch(SQLHSTMT StatementHandle) {
	return WithStatementResult(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!stmt->open) {
			return SQL_NO_DATA;
		}
		if (!stmt->chunk || stmt->chunk_row >= stmt->chunk->size() - 1) {
			// TODO try /catch
			try {
				stmt->chunk = stmt->res->Fetch();
			} catch (Exception &e) {
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
		for (idx_t col_idx = 0; col_idx < stmt->stmt->ColumnCount(); col_idx++) {
			auto bound_buf = stmt->bound_cols[col_idx];
			if (bound_buf.IsBound()) {
				if (!SQL_SUCCEEDED(SQLGetData(StatementHandle, col_idx + 1, bound_buf.type, bound_buf.ptr,
				                              bound_buf.len, bound_buf.strlen_or_ind))) {
					return SQL_ERROR;
				}
			}
		}

		D_ASSERT(stmt->chunk);
		D_ASSERT(stmt->chunk_row < stmt->chunk->size());
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLFetchScroll(SQLHSTMT StatementHandle, SQLSMALLINT FetchOrientation, SQLLEN FetchOffset) {

	if (FetchOrientation != SQL_FETCH_NEXT) {
		return SQL_ERROR;
	}
	return SQLFetch(StatementHandle);
}

SQLRETURN SQLRowCount(SQLHSTMT StatementHandle, SQLLEN *RowCountPtr) {
	return WithStatementResult(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!RowCountPtr) {
			return SQL_ERROR;
		}
		*RowCountPtr = -1; // we don't actually know most of the time
		return SQL_SUCCESS;
	});
}