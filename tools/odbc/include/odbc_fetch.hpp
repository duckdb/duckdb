#ifndef ODBC_FETCH_HPP
#define ODBC_FETCH_HPP

#include "duckdb.hpp"
#include "duckdb/common/windows.hpp"

#include <sqltypes.h>
#include <sqlext.h>

#include "duckdb/common/vector.hpp"
#include "duckdb/common/stack.hpp"

namespace duckdb {

struct OdbcHandleStmt;

class OdbcFetch {
public:
	const static SQLULEN SINGLE_VALUE_FETCH = 1;
	const static SQLRETURN RETURN_FETCH_BEFORE_START = 999;

	SQLULEN cursor_type;
	SQLULEN cursor_scrollable;
	SQLLEN row_count;

	struct {
		row_t col_idx;
		row_t row_idx;
		size_t length;
	} last_fetched_variable_val;

private:
	OdbcHandleStmt *hstmt_ref;
	// main structure to hold the fetched chunks
	vector<duckdb::unique_ptr<DataChunk>> chunks;
	// used by fetch prior
	duckdb::idx_t current_chunk_idx;
	duckdb::DataChunk *current_chunk;
	row_t chunk_row;
	row_t prior_chunk_row;

	// flag the end of the result set has reached
	// it's important because ODBC can reuse the result set many times
	bool resultset_end;

public:
	explicit OdbcFetch(OdbcHandleStmt *hstmt)
	    : cursor_type(SQL_CURSOR_FORWARD_ONLY), cursor_scrollable(SQL_NONSCROLLABLE), row_count(0), hstmt_ref(hstmt),
	      resultset_end(false) {
		ResetLastFetchedVariableVal();
	}
	~OdbcFetch();

	inline void AssertCurrentChunk() {
		D_ASSERT(chunk_row <= ((row_t)current_chunk->size()));
	}

	SQLRETURN Fetch(OdbcHandleStmt *hstmt, SQLULEN fetch_orientation, SQLLEN fetch_offset);

	SQLRETURN FetchFirst(OdbcHandleStmt *hstmt);

	SQLRETURN FetchNextChunk(SQLULEN fetch_orientation, OdbcHandleStmt *hstmt, SQLLEN fetch_offset);

	SQLRETURN DummyFetch();

	SQLRETURN GetValue(SQLUSMALLINT col_idx, Value &value);

	void ClearChunks();

	SQLRETURN Materialize(OdbcHandleStmt *hstmt);

	void ResetLastFetchedVariableVal();
	void SetLastFetchedVariableVal(row_t col_idx);
	void SetLastFetchedLength(size_t new_len);
	size_t GetLastFetchedLength();

	bool IsInExecutionState();

	SQLLEN GetRowCount();

private:
	SQLRETURN ColumnWise(OdbcHandleStmt *hstmt);

	SQLRETURN RowWise(OdbcHandleStmt *hstmt);

	inline bool RequireFetch() {
		return (chunks.empty() || (chunk_row >= ((duckdb::row_t)chunks.back()->size()) - 1));
	}

	void IncreaseRowCount();

	SQLRETURN FetchNext(OdbcHandleStmt *hstmt);

	SQLRETURN SetCurrentChunk(OdbcHandleStmt *hstmt);

	SQLRETURN SetPriorCurrentChunk(OdbcHandleStmt *hstmt);

	SQLRETURN BeforeStart();

	SQLRETURN SetAbsoluteCurrentChunk(OdbcHandleStmt *hstmt, SQLLEN fetch_offset);

	SQLRETURN SetFirstCurrentChunk(OdbcHandleStmt *hstmt);

	void SetRowStatus(idx_t row_idx, SQLINTEGER status);
};
} // namespace duckdb

#endif // ODBC_FETCH_HPP
