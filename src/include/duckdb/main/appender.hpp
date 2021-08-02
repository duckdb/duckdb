//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/appender.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {

class ClientContext;
class DuckDB;
class TableCatalogEntry;
class Connection;

//! The Appender class can be used to append elements to a table.
class Appender {
	//! The amount of chunks that will be gathered in the chunk collection before flushing
	static constexpr const idx_t FLUSH_COUNT = 100;

	//! A reference to a database connection that created this appender
	shared_ptr<ClientContext> context;
	//! The table description (including column names)
	unique_ptr<TableDescription> description;
	//! The append types
	vector<LogicalType> types;
	//! The buffered data for the append
	ChunkCollection collection;
	//! Internal chunk used for appends
	unique_ptr<DataChunk> chunk;
	//! The current column to append to
	idx_t column = 0;

public:
	DUCKDB_API Appender(Connection &con, const string &schema_name, const string &table_name);
	DUCKDB_API Appender(Connection &con, const string &table_name);
	DUCKDB_API ~Appender();

	//! Begins a new row append, after calling this the other AppendX() functions
	//! should be called the correct amount of times. After that,
	//! EndRow() should be called.
	DUCKDB_API void BeginRow();
	//! Finishes appending the current row.
	DUCKDB_API void EndRow();

	// Append functions
	template <class T>
	void Append(T value) {
		throw Exception("Undefined type for Appender::Append!");
	}

	DUCKDB_API void Append(const char *value, uint32_t length);

	// prepared statements
	template <typename... Args>
	void AppendRow(Args... args) {
		BeginRow();
		AppendRowRecursive(args...);
	}

	//! Commit the changes made by the appender.
	DUCKDB_API void Flush();
	//! Flush the changes made by the appender and close it. The appender cannot be used after this point
	DUCKDB_API void Close();

	DUCKDB_API vector<LogicalType> &GetTypes() {
		return types;
	}
	DUCKDB_API idx_t CurrentColumn() {
		return column;
	}

private:
	void InitializeChunk();
	void FlushChunk();

	template <class T>
	void AppendValueInternal(T value);
	template <class SRC, class DST>
	void AppendValueInternal(Vector &vector, SRC input);

	void AppendRowRecursive() {
		EndRow();
	}

	template <typename T, typename... Args>
	void AppendRowRecursive(T value, Args... args) {
		Append<T>(value);
		AppendRowRecursive(args...);
	}

	void AppendValue(const Value &value);
};

template <>
void DUCKDB_API Appender::Append(bool value);
template <>
void DUCKDB_API Appender::Append(int8_t value);
template <>
void DUCKDB_API Appender::Append(int16_t value);
template <>
void DUCKDB_API Appender::Append(int32_t value);
template <>
void DUCKDB_API Appender::Append(int64_t value);
template <>
void DUCKDB_API Appender::Append(uint8_t value);
template <>
void DUCKDB_API Appender::Append(uint16_t value);
template <>
void DUCKDB_API Appender::Append(uint32_t value);
template <>
void DUCKDB_API Appender::Append(uint64_t value);
template <>
void DUCKDB_API Appender::Append(float value);
template <>
void DUCKDB_API Appender::Append(double value);
template <>
void DUCKDB_API Appender::Append(date_t value);
template <>
void DUCKDB_API Appender::Append(dtime_t value);
template <>
void DUCKDB_API Appender::Append(timestamp_t value);
template <>
void DUCKDB_API Appender::Append(const char *value);
template <>
void DUCKDB_API Appender::Append(string_t value);
template <>
void DUCKDB_API Appender::Append(Value value);
template <>
void DUCKDB_API Appender::Append(std::nullptr_t value);

} // namespace duckdb
