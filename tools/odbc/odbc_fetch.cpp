#include "odbc_fetch.hpp"
#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

using duckdb::OdbcFetch;
using duckdb::OdbcHandleStmt;

OdbcFetch::~OdbcFetch() {
}

SQLRETURN OdbcFetch::Fetch(SQLHSTMT statement_handle, OdbcHandleStmt *stmt) {
	// case there is no bound column
	if (stmt->bound_cols.empty()) {
		return SQL_SUCCESS;
	}

	if (orientation == FetchOrientation::COLUMN) {
		if (!SQL_SUCCEEDED(OdbcFetch::ColumnWise(statement_handle, stmt))) {
			stmt->error_messages.emplace_back("Column-wise fetching failed.");
			return SQL_ERROR;
		}
	} else {
		if (!SQL_SUCCEEDED(duckdb::OdbcFetch::RowWise(statement_handle, stmt))) {
			stmt->error_messages.emplace_back("Row-wise fetching failed.");
			return SQL_ERROR;
		}
	}
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::ColumnWise(SQLHSTMT statement_handle, OdbcHandleStmt *stmt) {
	// now fill buffers in fetch if set
	// TODO actually vectorize this
	for (duckdb::idx_t col_idx = 0; col_idx < stmt->stmt->ColumnCount(); col_idx++) {
		auto bound_buf = stmt->bound_cols[col_idx];
		if (bound_buf.IsBound()) {
			if (!SQL_SUCCEEDED(duckdb::GetDataStmtResult(statement_handle, col_idx + 1, bound_buf.type, bound_buf.ptr,
			                                             bound_buf.len, bound_buf.strlen_or_ind))) {
				return SQL_ERROR;
			}
		}
	}
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::RowWise(SQLHSTMT statement_handle, OdbcHandleStmt *stmt) {
	SQLRETURN ret = SQL_SUCCESS;

	SQLULEN row_size = *(SQLULEN *)this->row_length;
	SQLULEN last_row_to_fetch = stmt->chunk_row + this->rows_to_fetch;
	if (last_row_to_fetch > stmt->chunk->size()) {
		last_row_to_fetch = stmt->chunk->size();
	}

	for (SQLULEN row_idx = stmt->chunk_row; row_idx < last_row_to_fetch; ++row_idx) {
		this->row_status_buff[row_idx] = SQL_SUCCESS;
		auto row_offeset = row_size * row_idx;
		for (duckdb::idx_t col_idx = 0; col_idx < stmt->stmt->ColumnCount(); col_idx++) {
			auto bound_col = stmt->bound_cols[col_idx];
			if (bound_col.IsBound()) {

				// the addresses must be byte addressable for row offset pointer arithmetic
				uint8_t *target_val_addr = (uint8_t *)bound_col.ptr + row_offeset;
				uint8_t *target_len_addr = (uint8_t *)bound_col.strlen_or_ind + row_offeset;

				if (!SQL_SUCCEEDED(duckdb::GetDataStmtResult(statement_handle, col_idx + 1, bound_col.type,
				                                             target_val_addr, bound_col.len,
				                                             (SQLLEN *)target_len_addr))) {
					if (this->row_status_buff) {
						this->row_status_buff[row_idx] = SQL_ROW_ERROR;
					}
					stmt->error_messages.emplace_back("Error retriving #row: " + std::to_string(row_idx) +
					                                  " and column: " + stmt->stmt->GetNames()[col_idx]);
					ret = SQL_SUCCESS_WITH_INFO;
				}
			}
		}
		stmt->chunk_row++;
	}
	*stmt->rows_fetched_ptr = last_row_to_fetch;

	return ret;
}