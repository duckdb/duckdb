#ifndef ODBC_FETCH_HPP
#define ODBC_FETCH_HPP

#include "duckdb.hpp"
#include <sqltypes.h>
#include <sql.h>
#include <sqlext.h>
#include <vector>

namespace duckdb {

class OdbcHandleStmt;

enum class FetchBindingOrientation : uint8_t { COLUMN = 0, ROW = 1 };

class OdbcFetch {
public:
	FetchBindingOrientation bind_orientation;
	SQLULEN rows_to_fetch;
	SQLPOINTER row_length;
	SQLUSMALLINT *row_status_buff;

	const static SQLULEN SINGLE_VALUE_FETCH = 1;

	SQLULEN cursor_type;
	SQLLEN row_count;

private:
	std::vector<unique_ptr<DataChunk>> chunks;
	duckdb::DataChunk *current_chunk;
	row_t chunk_row;

public:
	OdbcFetch()
	    : bind_orientation(FetchBindingOrientation::COLUMN), rows_to_fetch(SINGLE_VALUE_FETCH),
	      row_status_buff(nullptr), cursor_type(SQL_CURSOR_FORWARD_ONLY), row_count(0) {
	}
	~OdbcFetch();

	SQLRETURN Fetch(SQLHSTMT statement_handle, OdbcHandleStmt *stmt, SQLULEN fetch_orientation = SQL_FETCH_NEXT);

	SQLRETURN FetchNextChunk(SQLULEN fetch_orientation, OdbcHandleStmt *stmt);

	SQLRETURN GetValue(SQLUSMALLINT col_idx, Value &value);

	void ClearChunks();

	inline void AssertCurrentChunk() {
		D_ASSERT(chunk_row <= ((row_t)current_chunk->size()));
	}

private:
	SQLRETURN ColumnWise(SQLHSTMT statement_handle, OdbcHandleStmt *stmt);

	SQLRETURN RowWise(SQLHSTMT statement_handle, OdbcHandleStmt *stmt);

	inline bool NeedFetch() {
		return (chunks.empty() || (chunk_row >= ((duckdb::row_t)chunks.back()->size()) - 1));
	}

	SQLRETURN SetCurrentChunk(OdbcHandleStmt *stmt);
};
} // namespace duckdb

#endif // ODBC_FETCH_HPP
