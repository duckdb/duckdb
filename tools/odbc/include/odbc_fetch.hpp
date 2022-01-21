#ifndef ODBC_FETCH_HPP
#define ODBC_FETCH_HPP

#include "duckdb.hpp"
#include "duckdb/common/windows.hpp"

#include <sqltypes.h>
#include <sqlext.h>
#include <vector>
#include <stack>

namespace duckdb {

struct OdbcHandleStmt;

enum class FetchBindingOrientation : uint8_t { COLUMN = 0, ROW = 1 };

class OdbcFetch {
public:
	FetchBindingOrientation bind_orientation;
	SQLULEN rowset_size;
	SQLPOINTER row_length;
	SQLUSMALLINT *row_status_buff;

	const static SQLULEN SINGLE_VALUE_FETCH = 1;
	const static SQLRETURN RETURN_FETCH_BEFORE_START = 999;

	SQLULEN cursor_type;
	SQLLEN row_count;

	struct {
		row_t col_idx;
		row_t row_idx;
		size_t length;
	} last_fetched_variable_val;

private:
	// main structure to hold the fetched chunks
	std::vector<unique_ptr<DataChunk>> chunks;
	// used by fetch prior
	duckdb::idx_t current_chunk_idx;
	duckdb::DataChunk *current_chunk;
	row_t chunk_row;
	row_t prior_chunk_row;

	// flag the end of the result set has reached
	// it's important because ODBC can reuse the result set many times
	bool resultset_end;

public:
	OdbcFetch()
	    : bind_orientation(FetchBindingOrientation::COLUMN), rowset_size(SINGLE_VALUE_FETCH), row_status_buff(nullptr),
	      cursor_type(SQL_CURSOR_FORWARD_ONLY), row_count(0), resultset_end(false) {

		ResetLastFetchedVariableVal();
	}
	~OdbcFetch();

	inline void AssertCurrentChunk() {
		D_ASSERT(chunk_row <= ((row_t)current_chunk->size()));
	}

	SQLRETURN Fetch(SQLHSTMT statement_handle, OdbcHandleStmt *stmt, SQLULEN fetch_orientation = SQL_FETCH_NEXT,
	                SQLLEN fetch_offset = 0);

	SQLRETURN FetchFirst(SQLHSTMT statement_handle, OdbcHandleStmt *stmt);

	SQLRETURN FetchNextChunk(SQLULEN fetch_orientation, OdbcHandleStmt *stmt, SQLLEN fetch_offset);

	SQLRETURN GetValue(SQLUSMALLINT col_idx, Value &value);

	void ClearChunks();

	SQLRETURN Materialize(OdbcHandleStmt *stmt);

	void ResetLastFetchedVariableVal();
	void SetLastFetchedVariableVal(row_t col_idx);
	void SetLastFetchedLength(size_t new_len);
	size_t GetLastFetchedLength();

	bool IsInExecutionState();

private:
	SQLRETURN ColumnWise(SQLHSTMT statement_handle, OdbcHandleStmt *stmt);

	SQLRETURN RowWise(SQLHSTMT statement_handle, OdbcHandleStmt *stmt);

	inline bool RequireFetch() {
		return (chunks.empty() || (chunk_row >= ((duckdb::row_t)chunks.back()->size()) - 1));
	}

	void IncreaseRowCount();

	SQLRETURN FetchNext(OdbcHandleStmt *stmt);

	SQLRETURN SetCurrentChunk(OdbcHandleStmt *stmt);

	SQLRETURN SetPriorCurrentChunk(OdbcHandleStmt *stmt);

	SQLRETURN BeforeStart();

	SQLRETURN SetAbsoluteCurrentChunk(OdbcHandleStmt *stmt, SQLLEN fetch_offset);

	SQLRETURN SetFirstCurrentChunk(OdbcHandleStmt *stmt);
};
} // namespace duckdb

#endif // ODBC_FETCH_HPP
