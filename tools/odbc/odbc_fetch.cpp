#include "odbc_fetch.hpp"
// #include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "api_info.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

using duckdb::OdbcFetch;
using duckdb::OdbcHandleStmt;

OdbcFetch::~OdbcFetch() {
}

void OdbcFetch::IncreaseRowCount() {
	if ((chunk_row + rowset_size) < current_chunk->size()) {
		row_count += rowset_size;
	} else {
		row_count += current_chunk->size() - chunk_row;
	}
}

SQLRETURN OdbcFetch::FetchNext(OdbcHandleStmt *stmt) {
	try {
		auto chunk = stmt->res->Fetch();
		if (!chunk) {
			resultset_end = true;
			return SQL_NO_DATA;
		}
		if (!chunks.empty()) {
			// only push the prior chunk because the new one is already "popped" to the current chunk
			stack_prior_chunks.push(chunks.back().get());
		}
		chunks.emplace_back(move(chunk));
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

SQLRETURN OdbcFetch::SetCurrentChunk(OdbcHandleStmt *stmt) {
	if (chunks.empty()) {
		return SQL_NO_DATA;
	}
	// get always the last chunk and set as the current one
	current_chunk = chunks.back().get();
	chunk_row = prior_chunk_row = -1;

	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::SetPriorCurrentChunk(OdbcHandleStmt *stmt) {
	prior_chunk_row -= rowset_size;

	if (stack_prior_chunks.empty() && prior_chunk_row < -1) {
		chunk_row = -1;
		return SQL_NO_DATA;
	}

	if (prior_chunk_row < -1) {
		current_chunk = stack_prior_chunks.top();
		stack_prior_chunks.pop();
		prior_chunk_row = current_chunk->size() - rowset_size - 1;
		if (prior_chunk_row < 0) {
			prior_chunk_row = -1;
		}
	}

	chunk_row = prior_chunk_row;

	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::FetchFromBOF() {
	current_chunk = chunks.front().get();
	chunk_row = prior_chunk_row = -1;
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::SetAbsoluteCurrentChunk(OdbcHandleStmt *stmt, SQLLEN fetch_offset) {
	// there is no current_chunk, it requires fetch next
	if (!current_chunk || chunks.empty()) {
		FetchNext(stmt);
	}
	// it has reachted the last row
	if (fetch_offset > (SQLLEN)current_chunk->size() && resultset_end) {
		return SQL_NO_DATA;
	}

	// FetchOffset < 0 AND | FetchOffset | <= LastResultRow
	if ((fetch_offset < 0) && ((duckdb::idx_t)std::abs(fetch_offset) <= current_chunk->size())) {
		// LastResultRow + FetchOffset + 1
		chunk_row = chunk_row + fetch_offset + 1;
		if (chunk_row < -1) {
			chunk_row = -1;
		}
		prior_chunk_row = chunk_row;
		return SQL_SUCCESS;
	}

	// FetchOffset < 0    AND | FetchOffset |    > LastResultRow AND |FetchOffset | > RowsetSize
	if ((fetch_offset < 0) && (std::abs(fetch_offset) > chunk_row) && ((SQLULEN)std::abs(fetch_offset) > rowset_size)) {
		// Before start, return to BOF
		return FetchFromBOF();
	}

	// FetchOffset < 0    AND | FetchOffset |   > LastResultRow AND | FetchOffset | <= RowsetSize
	if ((fetch_offset < 0) && (std::abs(fetch_offset) > chunk_row) &&
	    ((SQLULEN)std::abs(fetch_offset) <= rowset_size)) {
		chunk_row = prior_chunk_row = 0;
		return SQL_SUCCESS;
	}

	// FetchOffset = 0, i.e.,return to BOF
	if (fetch_offset == 0) {
		if (!chunks.empty()) {
			return FetchFromBOF();
		}
		return FetchNext(stmt);
	}

	// 1 <= FetchOffset <= LastResultRow
	if ((fetch_offset >= 0) && (fetch_offset < (SQLLEN)current_chunk->size())) {
		chunk_row = fetch_offset - 1;
		prior_chunk_row = chunk_row;
		return SQL_SUCCESS;
	}

	// FetchOffset > LastResultRow
	if (fetch_offset >= (SQLLEN)current_chunk->size()) {
		return FetchNext(stmt);
	}

	return SQL_ERROR;
}

SQLRETURN OdbcFetch::FetchNextChunk(SQLULEN fetch_orientation, OdbcHandleStmt *stmt, SQLLEN fetch_offset) {
	if (cursor_type == SQL_CURSOR_FORWARD_ONLY || fetch_orientation == SQL_FETCH_NEXT) {
		prior_chunk_row = chunk_row;

		if (RequireFetch()) {
			if (resultset_end) {
				return SQL_NO_DATA;
			}
			return FetchNext(stmt);
		}
	}

	if (fetch_orientation == SQL_FETCH_PRIOR) {
		return SetPriorCurrentChunk(stmt);
	}

	if (fetch_orientation == SQL_FETCH_ABSOLUTE) {
		return SetAbsoluteCurrentChunk(stmt, fetch_offset);
	}

	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::GetValue(SQLUSMALLINT col_idx, duckdb::Value &value) {
	if (!current_chunk) {
		// TODO could throw an exception instead
		return SQL_ERROR;
	}
	value = current_chunk->GetValue(col_idx, chunk_row);
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::Fetch(SQLHSTMT statement_handle, OdbcHandleStmt *stmt, SQLULEN fetch_orientation,
                           SQLLEN fetch_offset) {
	SQLRETURN ret = FetchNextChunk(fetch_orientation, stmt, fetch_offset);
	if (ret != SQL_SUCCESS) {
		return ret;
	}

	// case there is no bound column
	if (stmt->bound_cols.empty()) {
		// increment fetched row
		chunk_row += rowset_size;
		if (stmt->rows_fetched_ptr) {
			(*stmt->rows_fetched_ptr) = 1;
		}
		IncreaseRowCount();
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
	IncreaseRowCount();
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::ColumnWise(SQLHSTMT statement_handle, OdbcHandleStmt *stmt) {
	SQLRETURN ret = SQL_SUCCESS;

	SQLLEN first_row_to_fetch = chunk_row + 1;
	SQLULEN last_row_to_fetch = first_row_to_fetch + this->rowset_size;

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

				if (this->rowset_size != SINGLE_VALUE_FETCH) {
					// need specialized pointer arithmetic according with the value type
					auto pointer_size = ApiInfo::PointerSizeOf(bound_col.type);
					if (pointer_size < 0) {
						pointer_size = bound_col.len;
					}
					target_val_addr = (uint8_t *)target_val_addr + (row_idx * pointer_size);
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
	SQLULEN last_row_to_fetch = first_row_to_fetch + this->rowset_size;

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
	while (!stack_prior_chunks.empty()) {
		stack_prior_chunks.pop();
	}
	chunks.clear();
	current_chunk = nullptr;
	chunk_row = prior_chunk_row = -1;
	row_count = 0;
	resultset_end = false;
}
