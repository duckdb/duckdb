#include "odbc_fetch.hpp"
// #include "duckdb_odbc.hpp"
#include "statement_functions.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

using duckdb::OdbcFetch;
using duckdb::OdbcHandleStmt;

OdbcFetch::~OdbcFetch() {
}

SQLRETURN OdbcFetch::SetCurrentChunk(OdbcHandleStmt *stmt) {
	// get always the last chunk and set as the current one
	current_chunk = chunks.back().get();
	if (!current_chunk) {
		stmt->open = false;
		return SQL_NO_DATA;
	}
	// count rows processed
	if (stmt->res->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
		row_count += ((duckdb::MaterializedQueryResult *)stmt->res.get())->collection.Count();
	} else {
		row_count += current_chunk->size();
	}
	chunk_row = -1;
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::FetchNextChunk(SQLULEN fetch_orientation, OdbcHandleStmt *stmt) {
	if (cursor_type == SQL_CURSOR_FORWARD_ONLY && NeedFetch()) {
		try {
			chunks.emplace_back(stmt->res->Fetch());
		} catch (duckdb::Exception &e) {
			// TODO this is quite dirty, we should have separate error holder
			stmt->res->error = e.what();
			stmt->res->success = false;
			stmt->open = false;
			stmt->error_messages.emplace_back(std::string(e.what()));
			return SQL_ERROR;
		}
		return SetCurrentChunk(stmt);
	}

	if (fetch_orientation == SQL_FETCH_PRIOR && cursor_type == SQL_CURSOR_STATIC) {
		if (chunks.empty()) {
			chunks.emplace_back(stmt->res->Fetch());
		}
		return SetCurrentChunk(stmt);
	}

	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::GetValue(SQLUSMALLINT col_idx, duckdb::Value &value) {
	if (!current_chunk) {
		// TODO could throught an exception instead
		return SQL_ERROR;
	}
	value = current_chunk->GetValue(col_idx, chunk_row);
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::Fetch(SQLHSTMT statement_handle, OdbcHandleStmt *stmt, SQLULEN fetch_orientation) {
	SQLRETURN ret = FetchNextChunk(fetch_orientation, stmt);
	if (ret != SQL_SUCCESS) {
		return ret;
	}
	// case there is no bound column
	if (stmt->bound_cols.empty()) {
		// increment fetched row
		chunk_row++;
		if (stmt->rows_fetched_ptr) {
			(*stmt->rows_fetched_ptr) = 1;
		}
		return SQL_SUCCESS;
	}

	if (bind_orientation == FetchBindingOrientation::COLUMN) {
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
	SQLRETURN ret = SQL_SUCCESS;

	SQLLEN first_row_to_fetch = chunk_row + 1;
	SQLULEN last_row_to_fetch = first_row_to_fetch + this->rows_to_fetch;

	if (last_row_to_fetch > current_chunk->size()) {
		last_row_to_fetch = current_chunk->size();
	}

	for (SQLULEN row_idx = first_row_to_fetch; row_idx < last_row_to_fetch; ++row_idx) {
		++chunk_row;

		if (this->row_status_buff) {
			this->row_status_buff[row_idx] = SQL_SUCCESS;
		}
		// now fill buffers in fetch if set
		// TODO actually vectorize this
		for (duckdb::idx_t col_idx = 0; col_idx < stmt->stmt->ColumnCount(); col_idx++) {
			auto bound_col = stmt->bound_cols[col_idx];
			if (bound_col.IsBound()) {

				auto target_val_addr = bound_col.ptr;
				auto target_len_addr = bound_col.strlen_or_ind;

				// check
				if (this->rows_to_fetch != SINGLE_VALUE_FETCH) {
					target_val_addr += row_idx;
					target_len_addr += row_idx;
				}

				if (!SQL_SUCCEEDED(duckdb::GetDataStmtResult(statement_handle, col_idx + 1, bound_col.type,
				                                             target_val_addr, bound_col.len, target_len_addr))) {

					if (this->row_status_buff) {
						this->row_status_buff[row_idx] = SQL_ROW_ERROR;
					}
					stmt->error_messages.emplace_back("Error retriving #row: " + std::to_string(row_idx) +
					                                  " and column: " + stmt->stmt->GetNames()[col_idx]);
					ret = SQL_SUCCESS_WITH_INFO;
				}
			}
		}
	}

	if (stmt->rows_fetched_ptr) {
		*stmt->rows_fetched_ptr = last_row_to_fetch - first_row_to_fetch;
	}

	return ret;
}

SQLRETURN OdbcFetch::RowWise(SQLHSTMT statement_handle, OdbcHandleStmt *stmt) {
	SQLRETURN ret = SQL_SUCCESS;
	SQLULEN row_size = *(SQLULEN *)this->row_length;

	SQLLEN first_row_to_fetch = chunk_row + 1;
	SQLULEN last_row_to_fetch = first_row_to_fetch + this->rows_to_fetch;

	if (last_row_to_fetch > current_chunk->size()) {
		last_row_to_fetch = current_chunk->size();
	}

	SQLULEN rows_fetched = 0;
	for (SQLULEN row_idx = first_row_to_fetch; row_idx < last_row_to_fetch; ++row_idx, ++rows_fetched) {
		++chunk_row;

		if (this->row_status_buff) {
			this->row_status_buff[row_idx] = SQL_SUCCESS;
		}
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
	}

	if (stmt->rows_fetched_ptr) {
		*stmt->rows_fetched_ptr = rows_fetched;
	}

	return ret;
}

void OdbcFetch::ClearChunks() {
	chunks.clear();
	current_chunk = nullptr;
	chunk_row = -1;
	row_count = 0;
}
