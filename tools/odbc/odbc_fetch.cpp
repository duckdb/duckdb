#include "odbc_fetch.hpp"
#include "duckdb_odbc.hpp"
#include "api_info.hpp"
#include "row_descriptor.hpp"
#include "statement_functions.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

using duckdb::OdbcFetch;
using duckdb::OdbcHandleStmt;

OdbcFetch::~OdbcFetch() {
	chunks.clear();
}

void OdbcFetch::IncreaseRowCount() {
	if ((chunk_row + hstmt_ref->row_desc->ard->header.sql_desc_array_size) < current_chunk->size()) {
		row_count += hstmt_ref->row_desc->ard->header.sql_desc_array_size;
	} else {
		row_count += current_chunk->size() - chunk_row;
	}
}

SQLRETURN OdbcFetch::Materialize(OdbcHandleStmt *hstmt) {
	// preserve states before materialization
	auto before_cur_chunk = current_chunk;
	auto before_cur_chunk_idx = current_chunk_idx;
	auto before_chunk_row = chunk_row;
	auto before_prior_chunk_row = prior_chunk_row;

	SQLRETURN ret;
	do {
		ret = FetchNext(hstmt);
	} while (SQL_SUCCEEDED(ret));

	D_ASSERT(resultset_end);

	// restore states
	if (before_cur_chunk) {
		current_chunk = before_cur_chunk;
		current_chunk_idx = before_cur_chunk_idx;
		chunk_row = before_chunk_row;
		prior_chunk_row = before_prior_chunk_row;
	} else {
		if (!chunks.empty()) {
			current_chunk = chunks.front().get();
		}
		current_chunk_idx = 0;
		chunk_row = prior_chunk_row = -1;
	}

	if (ret == SQL_NO_DATA || ret == SQL_SUCCESS) {
		return SQL_SUCCESS;
	}

	return ret;
}

SQLRETURN OdbcFetch::FetchNext(OdbcHandleStmt *hstmt) {
	// case hasn't reached the end of query result, then try to fetch
	if (!resultset_end) {
		try {
			// it's need to reset the last_fetched_len
			ResetLastFetchedVariableVal();
			auto chunk = hstmt->res->Fetch();
			if (hstmt->res->HasError()) {
				hstmt->open = false;
				hstmt->error_messages.emplace_back(hstmt->res->GetError());
				return SQL_ERROR;
			}
			if (!chunk || chunk->size() == 0) {
				resultset_end = true;
				return SQL_NO_DATA;
			}
			if (cursor_type == SQL_CURSOR_FORWARD_ONLY) {
				if (!chunks.empty()) {
					// holding only one chunk to avoid full materialization in memory
					chunks.clear();
				}
				chunks.emplace_back(std::move(chunk));
			} else {
				chunks.emplace_back(std::move(chunk));
			}
		} catch (duckdb::Exception &e) {
			// TODO this is quite dirty, we should have separate error holder
			hstmt->res->SetError(PreservedError(e));
			hstmt->open = false;
			hstmt->error_messages.emplace_back(std::string(e.what()));
			return SQL_ERROR;
		}
	}

	return SetCurrentChunk(hstmt);
}

SQLRETURN OdbcFetch::SetCurrentChunk(OdbcHandleStmt *hstmt) {
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

SQLRETURN OdbcFetch::SetPriorCurrentChunk(OdbcHandleStmt *hstmt) {
	prior_chunk_row -= hstmt_ref->row_desc->ard->header.sql_desc_array_size;

	// case the current chunk is the first one and fetch prior has reached the position "before start"
	if (current_chunk_idx == 0 && prior_chunk_row < -1) {
		chunk_row = -1;
		return SQL_NO_DATA;
	}

	if (prior_chunk_row < -1) {
		--current_chunk_idx;
		current_chunk = chunks[current_chunk_idx].get();
		prior_chunk_row = current_chunk->size() - hstmt_ref->row_desc->ard->header.sql_desc_array_size - 1;
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

SQLRETURN OdbcFetch::SetAbsoluteCurrentChunk(OdbcHandleStmt *hstmt, SQLLEN fetch_offset) {
	// there is no current_chunk, it requires fetch next
	if (!current_chunk || chunks.empty()) {
		FetchNext(hstmt);
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
	if ((fetch_offset < 0) && (std::abs(fetch_offset) > chunk_row) &&
	    ((SQLULEN)std::abs(fetch_offset) > hstmt_ref->row_desc->ard->header.sql_desc_array_size)) {
		// Before start, return to BOF
		return BeforeStart();
	}

	// FetchOffset < 0    AND | FetchOffset |   > LastResultRow AND | FetchOffset | <= RowsetSize
	if ((fetch_offset < 0) && (std::abs(fetch_offset) > chunk_row) &&
	    ((SQLULEN)std::abs(fetch_offset) <= hstmt_ref->row_desc->ard->header.sql_desc_array_size)) {
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
		auto ret = FetchNext(hstmt);
		if (ret != SQL_SUCCESS) {
			current_chunk_idx = 0; // reset chunk idx
			return ret;
		}
		fetch_offset -= current_chunk->size();
		// recall set absolute based on the new chunk and relative offset
		return SetAbsoluteCurrentChunk(hstmt, fetch_offset);
	}

	return SQL_ERROR;
}

SQLRETURN OdbcFetch::SetFirstCurrentChunk(OdbcHandleStmt *hstmt) {
	BeforeStart();
	if (!current_chunk) {
		return FetchNext(hstmt);
	}
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::FetchNextChunk(SQLULEN fetch_orientation, OdbcHandleStmt *hstmt, SQLLEN fetch_offset) {
	if (cursor_type == SQL_CURSOR_FORWARD_ONLY && fetch_orientation != SQL_FETCH_NEXT) {
		hstmt->error_messages.emplace_back("Incorrect fetch orientation for cursor type: SQL_CURSOR_FORWARD_ONLY.");
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
			return FetchNext(hstmt);
		}
		return SQL_SUCCESS;
	case SQL_FETCH_PRIOR:
		return SetPriorCurrentChunk(hstmt);
	case SQL_FETCH_ABSOLUTE:
		return SetAbsoluteCurrentChunk(hstmt, fetch_offset);
	case SQL_FETCH_FIRST:
		return SetFirstCurrentChunk(hstmt);
	default:
		return SQL_SUCCESS;
	}
}

SQLRETURN OdbcFetch::DummyFetch() {
	if (hstmt_ref->retrieve_data == SQL_RD_OFF) {
		auto row_set_size = (SQLLEN)hstmt_ref->row_desc->ard->header.sql_desc_array_size;

		if (hstmt_ref->odbc_fetcher->chunk_row + row_set_size > hstmt_ref->odbc_fetcher->row_count) {
			row_set_size = hstmt_ref->odbc_fetcher->row_count - hstmt_ref->odbc_fetcher->chunk_row;
		}
		if (row_set_size <= 0) {
			return SQL_NO_DATA;
		}
		if (hstmt_ref->row_desc->ird->header.sql_desc_array_status_ptr) {
			duckdb::idx_t row_idx;
			for (row_idx = 0; row_idx < (duckdb::idx_t)row_set_size; row_idx++) {
				hstmt_ref->row_desc->ird->header.sql_desc_array_status_ptr[row_idx] = SQL_ROW_SUCCESS;
			}
			for (; row_idx < hstmt_ref->row_desc->ard->header.sql_desc_array_size; row_idx++) {
				hstmt_ref->row_desc->ird->header.sql_desc_array_status_ptr[row_idx] = SQL_ROW_NOROW;
			}
		}
		return SQL_SUCCESS;
	}
	return SQL_NEED_DATA;
}

SQLRETURN OdbcFetch::GetValue(SQLUSMALLINT col_idx, duckdb::Value &value) {
	if (!current_chunk) {
		// TODO could throw an exception instead
		return SQL_ERROR;
	}
	value = current_chunk->GetValue(col_idx, chunk_row);
	return SQL_SUCCESS;
}

SQLRETURN OdbcFetch::Fetch(OdbcHandleStmt *hstmt, SQLULEN fetch_orientation, SQLLEN fetch_offset) {
	SQLRETURN ret = FetchNextChunk(fetch_orientation, hstmt, fetch_offset);
	if (ret != SQL_SUCCESS) {
		if (ret == RETURN_FETCH_BEFORE_START) {
			return SQL_SUCCESS;
		}
		return ret;
	}

	// case there is no bound column
	if (hstmt->bound_cols.empty()) {
		// increment fetched row
		chunk_row += hstmt_ref->row_desc->ard->header.sql_desc_array_size;
		if (hstmt->rows_fetched_ptr) {
			(*hstmt->rows_fetched_ptr) = 1;
		}
		IncreaseRowCount();
		return SQL_SUCCESS;
	}

	if (hstmt_ref->row_desc->ard->header.sql_desc_bind_type == SQL_BIND_BY_COLUMN) {
		ret = OdbcFetch::ColumnWise(hstmt);
		if (!SQL_SUCCEEDED(ret)) {
			hstmt->error_messages.emplace_back("Column-wise fetching failed.");
			return SQL_ERROR;
		}
	} else {
		// sql_desc_bind_type should be greater than 0 because it contains the length of the row to be fetched
		D_ASSERT(hstmt->row_desc->ard->header.sql_desc_bind_type > 0);
		if (!SQL_SUCCEEDED(duckdb::OdbcFetch::RowWise(hstmt))) {
			hstmt->error_messages.emplace_back("Row-wise fetching failed.");
			return SQL_ERROR;
		}
	}
	IncreaseRowCount();
	return ret;
}

SQLRETURN OdbcFetch::FetchFirst(OdbcHandleStmt *hstmt) {
	auto cursor_type_before = cursor_type;
	cursor_type = SQL_CURSOR_DYNAMIC;
	auto ret = FetchNextChunk(SQL_FETCH_FIRST, hstmt, 0);
	cursor_type = cursor_type_before;
	return ret;
}

SQLRETURN OdbcFetch::ColumnWise(OdbcHandleStmt *hstmt) {
	SQLRETURN ret = SQL_SUCCESS;

	SQLLEN first_row_to_fetch = chunk_row + 1;
	SQLULEN last_row_to_fetch = first_row_to_fetch + hstmt_ref->row_desc->ard->header.sql_desc_array_size;

	if (last_row_to_fetch > current_chunk->size()) {
		last_row_to_fetch = current_chunk->size();
	}

	for (SQLULEN row_idx = first_row_to_fetch; row_idx < last_row_to_fetch; ++row_idx) {
		++chunk_row;
		SetRowStatus(row_idx, SQL_SUCCESS);
		// now fill buffers in fetch if set
		// TODO actually vectorize this
		for (duckdb::idx_t col_idx = 0; col_idx < hstmt->stmt->ColumnCount(); col_idx++) {
			auto bound_col = hstmt->bound_cols[col_idx];

			if (!bound_col.IsBound() && !bound_col.IsVarcharBound()) {
				continue;
			}
			auto target_val_addr = bound_col.ptr;
			auto target_len_addr = bound_col.strlen_or_ind;

			if (hstmt_ref->row_desc->ard->header.sql_desc_array_size != SINGLE_VALUE_FETCH) {
				// need specialized pointer arithmetic according to the value type
				auto pointer_size = ApiInfo::PointerSizeOf(bound_col.type);
				if (pointer_size < 0) {
					pointer_size = bound_col.len;
				}
				target_val_addr = (uint8_t *)target_val_addr + (row_idx * pointer_size);
				target_len_addr += row_idx;
			}

			if (!SQL_SUCCEEDED(duckdb::GetDataStmtResult(hstmt, col_idx + 1, bound_col.type, target_val_addr,
			                                             bound_col.len, target_len_addr))) {
				SetRowStatus(row_idx, SQL_ROW_ERROR);
				hstmt->error_messages.emplace_back("Error retrieving #row: " + std::to_string(row_idx) +
				                                   " and column: " + hstmt->stmt->GetNames()[col_idx]);
				ret = SQL_SUCCESS_WITH_INFO;
			}
		}
	}

	if (hstmt->rows_fetched_ptr) {
		*hstmt->rows_fetched_ptr = last_row_to_fetch - first_row_to_fetch;
	}

	return ret;
}

SQLRETURN OdbcFetch::RowWise(OdbcHandleStmt *hstmt) {
	SQLRETURN ret = SQL_SUCCESS;
	SQLULEN row_size = hstmt->row_desc->ard->header.sql_desc_bind_type;

	SQLLEN first_row_to_fetch = chunk_row + 1;
	SQLULEN last_row_to_fetch = first_row_to_fetch + hstmt_ref->row_desc->ard->header.sql_desc_array_size;

	if (last_row_to_fetch > current_chunk->size()) {
		last_row_to_fetch = current_chunk->size();
	}

	SQLULEN rows_fetched = 0;
	for (SQLULEN row_idx = first_row_to_fetch; row_idx < last_row_to_fetch; ++row_idx, ++rows_fetched) {
		++chunk_row;
		SetRowStatus(row_idx, SQL_SUCCESS);
		auto row_offset = row_size * row_idx;
		for (duckdb::idx_t col_idx = 0; col_idx < hstmt->stmt->ColumnCount(); col_idx++) {
			auto bound_col = hstmt->bound_cols[col_idx];
			if (!bound_col.IsBound()) {
				continue;
			}

			// the addresses must be byte addressable for row offset pointer arithmetic
			uint8_t *target_val_addr = (uint8_t *)bound_col.ptr + row_offset;
			uint8_t *target_len_addr = (uint8_t *)bound_col.strlen_or_ind + row_offset;

			if (!SQL_SUCCEEDED(duckdb::GetDataStmtResult(hstmt, col_idx + 1, bound_col.type, target_val_addr,
			                                             bound_col.len, (SQLLEN *)target_len_addr))) {
				SetRowStatus(row_idx, SQL_ROW_ERROR);
				hstmt->error_messages.emplace_back("Error retriving #row: " + std::to_string(row_idx) +
				                                   " and column: " + hstmt->stmt->GetNames()[col_idx]);
				ret = SQL_SUCCESS_WITH_INFO;
			}
		}
	}

	if (hstmt->rows_fetched_ptr) {
		*hstmt->rows_fetched_ptr = rows_fetched;
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

void OdbcFetch::ResetLastFetchedVariableVal() {
	last_fetched_variable_val.col_idx = -1;
	last_fetched_variable_val.row_idx = -1;
	last_fetched_variable_val.length = 0;
}

void OdbcFetch::SetLastFetchedVariableVal(row_t col_idx) {
	if (last_fetched_variable_val.col_idx != col_idx || last_fetched_variable_val.row_idx != chunk_row) {
		last_fetched_variable_val.length = 0;
	}
	last_fetched_variable_val.col_idx = col_idx;
	last_fetched_variable_val.row_idx = chunk_row;
}

void OdbcFetch::SetLastFetchedLength(size_t new_len) {
	last_fetched_variable_val.length = new_len;
}

size_t OdbcFetch::GetLastFetchedLength() {
	return last_fetched_variable_val.length;
}

bool OdbcFetch::IsInExecutionState() {
	return !chunks.empty() && current_chunk != nullptr;
}

SQLLEN OdbcFetch::GetRowCount() {
	if (current_chunk) {
		return duckdb::MaxValue(row_count, (SQLLEN)current_chunk->size());
	}
	return row_count;
}

void OdbcFetch::SetRowStatus(idx_t row_idx, SQLINTEGER status) {
	if (hstmt_ref->row_desc->ird->header.sql_desc_array_status_ptr) {
		hstmt_ref->row_desc->ird->header.sql_desc_array_status_ptr[row_idx] = status;
	}
}
