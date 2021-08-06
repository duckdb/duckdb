#include "odbc_fetch.hpp"
#include "duckdb_odbc.hpp"
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

SQLRETURN OdbcFetch::Materialize(OdbcHandleStmt *stmt) {
	// preserve states before materialization
	auto before_cur_chunk = current_chunk;
	auto before_cur_chunk_idx = current_chunk_idx;
	auto before_chunk_row = chunk_row;
	auto before_prior_chunk_row = prior_chunk_row;

	SQLRETURN ret;
	do {
		ret = FetchNext(stmt);
	} while (SQL_SUCCEEDED(ret));

	// restore states
	current_chunk = before_cur_chunk;
	current_chunk_idx = before_cur_chunk_idx;
	chunk_row = before_chunk_row;
	prior_chunk_row = before_prior_chunk_row;

	if (ret == SQL_NO_DATA || ret == SQL_SUCCESS) {
		return SQL_SUCCESS;
	}

	return ret;
}

SQLRETURN OdbcFetch::FetchNext(OdbcHandleStmt *stmt) {
	// case hasn't reached the end of query result, then try to fetch
	if (!resultset_end) {
		try {
			auto chunk = stmt->res->Fetch();
			if (!chunk) {
				resultset_end = true;
				return SQL_NO_DATA;
			}
			if (cursor_type == SQL_CURSOR_FORWARD_ONLY) {
				if (!chunks.empty()) {
					// holding only one chunk to avoid full materialization in memory
					chunks.clear();
				}
				chunks.emplace_back(move(chunk));
			} else {
				chunks.emplace_back(move(chunk));
			}
		} catch (duckdb::Exception &e) {
			// TODO this is quite dirty, we should have separate error holder
			stmt->res->error = e.what();
			stmt->res->success = false;
			stmt->open = false;
			stmt->error_messages.emplace_back(std::string(e.what()));
			return SQL_ERROR;
		}
	}

	return SetCurrentChunk(stmt);
}

SQLRETURN OdbcFetch::SetCurrentChunk(OdbcHandleStmt *stmt) {
	if (chunks.empty()) {
		return SQL_NO_DATA;
	}
	if (cursor_type != SQL_CURSOR_FORWARD_ONLY && current_chunk_idx < (chunks.size() - 1)) {
		++current_chunk_idx;
		current_chunk = chunks[current_chunk_idx].get();
		chunk_row = prior_chunk_row = -1;
		return SQL_SUCCESS;
	}
	// get always the last chunk and set as the current one
	current_chunk = chunks.back().get();
	current_chunk_idx = chunks.size() - 1;
	chunk_row = prior_chunk_row = -1;

	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::SetPriorCurrentChunk(OdbcHandleStmt *stmt) {
	prior_chunk_row -= rowset_size;

	// case the current chunk is the first one and fetch prior has reached the position "before start"
	if (current_chunk_idx == 0 && prior_chunk_row < -1) {
		chunk_row = -1;
		return SQL_NO_DATA;
	}

	if (prior_chunk_row < -1) {
		--current_chunk_idx;
		current_chunk = chunks[current_chunk_idx].get();
		prior_chunk_row = current_chunk->size() - rowset_size - 1;
		if (prior_chunk_row < 0) {
			prior_chunk_row = -1;
		}
	}

	chunk_row = prior_chunk_row;

	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::BeforeStart() {
	if (!chunks.empty()) {
		current_chunk = chunks.front().get();
	}
	current_chunk_idx = 0;
	chunk_row = prior_chunk_row = -1;
	// it should stop
	// the cursor is just positioned at the begin of the result set without needed to fetch data
	return RETURN_FETCH_BEFORE_START;
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
		return BeforeStart();
	}

	// FetchOffset < 0    AND | FetchOffset |   > LastResultRow AND | FetchOffset | <= RowsetSize
	if ((fetch_offset < 0) && (std::abs(fetch_offset) > chunk_row) &&
	    ((SQLULEN)std::abs(fetch_offset) <= rowset_size)) {
		chunk_row = prior_chunk_row = 0;
		return SQL_SUCCESS;
	}

	// FetchOffset = 0, i.e.,return to BOF
	if (fetch_offset == 0) {
		return BeforeStart();
	}

	// 1 <= FetchOffset <= LastResultRow
	if ((fetch_offset >= 0) && (fetch_offset < (SQLLEN)current_chunk->size())) {
		chunk_row = fetch_offset - 1;
		prior_chunk_row = chunk_row;
		return SQL_SUCCESS;
	}

	// FetchOffset > LastResultRow
	if (fetch_offset >= (SQLLEN)current_chunk->size()) {
		auto ret = FetchNext(stmt);
		if (ret != SQL_SUCCESS) {
			current_chunk_idx = 0; // reset chunk idx
			return ret;
		}
		fetch_offset -= current_chunk->size();
		// recall set absolute based on the new chunk and relative offset
		return SetAbsoluteCurrentChunk(stmt, fetch_offset);
	}

	return SQL_ERROR;
}

SQLRETURN OdbcFetch::SetFirstCurrentChunk(OdbcHandleStmt *stmt) {
	BeforeStart();
	if (!current_chunk) {
		return FetchNext(stmt);
	}
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::FetchNextChunk(SQLULEN fetch_orientation, OdbcHandleStmt *stmt, SQLLEN fetch_offset) {
	if (cursor_type == SQL_CURSOR_FORWARD_ONLY && fetch_orientation != SQL_FETCH_NEXT) {
		stmt->error_messages.emplace_back("Incorrect fetch orientation for cursor type: SQL_CURSOR_FORWARD_ONLY.");
		return SQL_ERROR;
	}

	switch (fetch_orientation) {
	case SQL_FETCH_NEXT:
		prior_chunk_row = chunk_row;
		if (RequireFetch()) {
			// check if the fetch already reached the end of result set and the current chunk index is the last one
			if (resultset_end && (current_chunk_idx == chunks.size() - 1)) {
				return SQL_NO_DATA;
			}
			return FetchNext(stmt);
		}
		return SQL_SUCCESS;
	case SQL_FETCH_PRIOR:
		return SetPriorCurrentChunk(stmt);
	case SQL_FETCH_ABSOLUTE:
		return SetAbsoluteCurrentChunk(stmt, fetch_offset);
	case SQL_FETCH_FIRST:
		return SetFirstCurrentChunk(stmt);
	default:
		return SQL_SUCCESS;
	}
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
		if (ret == RETURN_FETCH_BEFORE_START) {
			return SQL_SUCCESS;
		}
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
			if (!bound_col.IsBound()) {
				continue;
			}
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

			if (!SQL_SUCCEEDED(duckdb::GetDataStmtResult(statement_handle, col_idx + 1, bound_col.type, target_val_addr,
			                                             bound_col.len, target_len_addr))) {

				if (this->row_status_buff) {
					this->row_status_buff[row_idx] = SQL_ROW_ERROR;
				}
				stmt->error_messages.emplace_back("Error retriving #row: " + std::to_string(row_idx) +
				                                  " and column: " + stmt->stmt->GetNames()[col_idx]);
				ret = SQL_SUCCESS_WITH_INFO;
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
			if (!bound_col.IsBound()) {
				continue;
			}

			// the addresses must be byte addressable for row offset pointer arithmetic
			uint8_t *target_val_addr = (uint8_t *)bound_col.ptr + row_offeset;
			uint8_t *target_len_addr = (uint8_t *)bound_col.strlen_or_ind + row_offeset;

			if (!SQL_SUCCEEDED(duckdb::GetDataStmtResult(statement_handle, col_idx + 1, bound_col.type, target_val_addr,
			                                             bound_col.len, (SQLLEN *)target_len_addr))) {
				if (this->row_status_buff) {
					this->row_status_buff[row_idx] = SQL_ROW_ERROR;
				}
				stmt->error_messages.emplace_back("Error retriving #row: " + std::to_string(row_idx) +
				                                  " and column: " + stmt->stmt->GetNames()[col_idx]);
				ret = SQL_SUCCESS_WITH_INFO;
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
	chunk_row = prior_chunk_row = -1;
	row_count = 0;
	resultset_end = false;
}