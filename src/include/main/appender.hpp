//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/appender.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "main/client_context.hpp"

namespace duckdb {

class ClientContext;
class DuckDB;
class TableCatalogEntry;

//! The Appender class can be used to append elements to a table.
class Appender {
	//! A reference to the database
	DuckDB &db;
	//! The client context
	ClientContext context;
	//! The table entry to append to
	TableCatalogEntry *table_entry;
	//! Internal chunk used for appends
	DataChunk chunk;
	//! The current column to append to
	index_t column = 0;

public:
	Appender(DuckDB &db, string schema_name, string table_name);
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
	void Commit();
	//! Rollback any changes made by the appender The appender cannot be used after this point.
	void Rollback();

	index_t CurrentColumn() {
		return column;
	}

private:
	void CheckAppend(TypeId type = TypeId::INVALID);
	//! Flushes all appends to the base table.
	void Flush();
};
} // namespace duckdb
