//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// main/appender.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"

namespace duckdb {

class ClientContext;
class DuckDBConnection;
class TableCatalogEntry;

//! The Appender class can be used to append elements to a table.
class Appender {
	friend class DuckDBConnection;
	//! A reference back to the connection that created this appender
	DuckDBConnection &connection;
	//! The client context
	ClientContext &context;
	//! The table entry to append to
	TableCatalogEntry *table_entry;
	//! Internal chunk used for appends
	DataChunk chunk;
	//! The current column to append to
	size_t column = 0;

  public:
	Appender(DuckDBConnection &connection, ClientContext &context,
	         TableCatalogEntry *table_entry);

	//! Begins a new row append, after calling this the other append_x functions
	//! should be called the correct amount of times. After that,
	//! end_append_row() should be called.
	void begin_append_row();

	// Append functions
	// Note that none of these functions (besides append_value) does type
	// conversion, using the wrong type for the wrong column will trigger an
	// assert

	//! Append a tinyint
	void append_tinyint(int8_t value);
	//! Append a smallint
	void append_smallint(int16_t value);
	//! Append an integer
	void append_int(int value);
	//! Append a bigint
	void append_bigint(int64_t value);
	//! Append a varchar
	void append_string(const char *value);
	//! Append a double
	void append_double(double value);
	//! Append a generic value. This is the only function of the append_X family
	//! that does conversion for you, but in exchange for lower efficiency.
	void append_value(Value value);

	//! Finishes appending the current row.
	void end_append_row();

	//! Destroy the appender. It cannot be used after this point.
	void destroy();

  private:
	void check_append(TypeId type = TypeId::INVALID);
	//! Flushes all appends to the base table.
	void flush();
};
} // namespace duckdb
