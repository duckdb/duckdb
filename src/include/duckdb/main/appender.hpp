//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/appender.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"

#include <mutex>

namespace duckdb {

class ClientContext;
class DuckDB;
class TableCatalogEntry;
class Connection;

//! The Appender class can be used to append elements to a table.
class Appender {
	//! A reference to a database connection that created this appender
	Connection &con;
	//! The table entry to append to
	TableCatalogEntry *table_entry;
	//! Internal chunk used for appends
	DataChunk chunk;
	//! The current column to append to
	index_t column = 0;
	//! Lock holder for appends
	std::unique_lock<std::mutex> lock;

public:
	Appender(Connection &con, string schema_name, string table_name, std::unique_lock<std::mutex> lock);

	~Appender();

	//! Begins a new row append, after calling this the other AppendX() functions
	//! should be called the correct amount of times. After that,
	//! EndRow() should be called.
	void BeginRow();
	//! Finishes appending the current row.
	void EndRow();

	// Append functions
	// Note that none of these functions (besides AppendValue) does type
	// conversion, using the wrong type for the wrong column will trigger an
	// assert
	//! Append a bool

	void AppendBoolean(int8_t value);

	//! Append a tinyint
	void AppendTinyInt(int8_t value);
	//! Append a smallint
	void AppendSmallInt(int16_t value);
	//! Append an integer
	void AppendInteger(int value);
	//! Append a bigint
	void AppendBigInt(int64_t value);
	//! Append a varchar
	void AppendString(const char *value);
	//! Append a double
	void AppendDouble(double value);
	//! Append a generic value. This is the only function of the append_X family
	//! that does conversion for you, but in exchange for lower efficiency.
	void AppendValue(Value value);

	//! Commit the changes made by the appender. The appender cannot be used after this point.
	void Flush();

	index_t CurrentColumn() {
		return column;
	}

private:
	void CheckAppend(TypeId type = TypeId::INVALID);
};
} // namespace duckdb
