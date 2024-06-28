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

namespace duckdb {

class ColumnDataCollection;
class ClientContext;
class DuckDB;
class TableCatalogEntry;
class Connection;

enum class AppenderType : uint8_t {
	LOGICAL, // Cast input -> LogicalType
	PHYSICAL // Cast input -> PhysicalType
};

//! The Appender class can be used to append elements to a table.
class BaseAppender {
public:
	//! The amount of tuples that will be gathered in the column data collection before flushing
	static constexpr const idx_t DEFAULT_FLUSH_COUNT = STANDARD_VECTOR_SIZE * 100ULL;

protected:
	Allocator &allocator;
	//! The append types
	vector<LogicalType> types;
	//! The buffered data for the append
	unique_ptr<ColumnDataCollection> collection;
	//! Internal chunk used for appends
	DataChunk chunk;
	//! The current column to append to
	idx_t column = 0;
	//! The type of the appender
	AppenderType appender_type;
	//! The amount of rows after which we flush the appender automatically
	idx_t flush_count = DEFAULT_FLUSH_COUNT;

protected:
	DUCKDB_API BaseAppender(Allocator &allocator, AppenderType type);
	DUCKDB_API BaseAppender(Allocator &allocator, vector<LogicalType> types, AppenderType type,
	                        idx_t flush_count = DEFAULT_FLUSH_COUNT);

public:
	DUCKDB_API virtual ~BaseAppender();

	//! Begins a new row append, after calling this the other AppendX() functions
	//! should be called the correct amount of times. After that,
	//! EndRow() should be called.
	DUCKDB_API void BeginRow();
	//! Finishes appending the current row.
	DUCKDB_API void EndRow();

	// Append functions
	template <class T>
	void Append(T value) = delete;

	DUCKDB_API void Append(const char *value, uint32_t length);

	// prepared statements
	template <typename... ARGS>
	void AppendRow(ARGS... args) {
		BeginRow();
		AppendRowRecursive(args...);
	}

	//! Commit the changes made by the appender.
	DUCKDB_API void Flush();
	//! Flush the changes made by the appender and close it. The appender cannot be used after this point
	DUCKDB_API void Close();

	vector<LogicalType> &GetTypes() {
		return types;
	}
	idx_t CurrentColumn() const {
		return column;
	}
	DUCKDB_API void AppendDataChunk(DataChunk &value);

protected:
	void Destructor();
	virtual void FlushInternal(ColumnDataCollection &collection) = 0;
	void InitializeChunk();
	void FlushChunk();

	template <class T>
	void AppendValueInternal(T value);
	template <class SRC, class DST>
	void AppendValueInternal(Vector &vector, SRC input);
	template <class SRC, class DST>
	void AppendDecimalValueInternal(Vector &vector, SRC input);

	void AppendRowRecursive() {
		EndRow();
	}

	template <typename T, typename... ARGS>
	void AppendRowRecursive(T value, ARGS... args) {
		Append<T>(value);
		AppendRowRecursive(args...);
	}

	void AppendValue(const Value &value);
};

class Appender : public BaseAppender {
	//! A reference to a database connection that created this appender
	shared_ptr<ClientContext> context;
	//! The table description (including column names)
	unique_ptr<TableDescription> description;
	//! The default expressions
	unordered_map<idx_t, Value> default_values;

public:
	DUCKDB_API Appender(Connection &con, const string &schema_name, const string &table_name);
	DUCKDB_API Appender(Connection &con, const string &table_name);
	DUCKDB_API ~Appender() override;

public:
	void AppendDefault();

protected:
	void FlushInternal(ColumnDataCollection &collection) override;
};

class InternalAppender : public BaseAppender {
	//! The client context
	ClientContext &context;
	//! The internal table entry to append to
	TableCatalogEntry &table;

public:
	DUCKDB_API InternalAppender(ClientContext &context, TableCatalogEntry &table,
	                            idx_t flush_count = DEFAULT_FLUSH_COUNT);
	DUCKDB_API ~InternalAppender() override;

protected:
	void FlushInternal(ColumnDataCollection &collection) override;
};

template <>
DUCKDB_API void BaseAppender::Append(bool value);
template <>
DUCKDB_API void BaseAppender::Append(int8_t value);
template <>
DUCKDB_API void BaseAppender::Append(int16_t value);
template <>
DUCKDB_API void BaseAppender::Append(int32_t value);
template <>
DUCKDB_API void BaseAppender::Append(int64_t value);
template <>
DUCKDB_API void BaseAppender::Append(hugeint_t value);
template <>
DUCKDB_API void BaseAppender::Append(uhugeint_t value);
template <>
DUCKDB_API void BaseAppender::Append(uint8_t value);
template <>
DUCKDB_API void BaseAppender::Append(uint16_t value);
template <>
DUCKDB_API void BaseAppender::Append(uint32_t value);
template <>
DUCKDB_API void BaseAppender::Append(uint64_t value);
template <>
DUCKDB_API void BaseAppender::Append(float value);
template <>
DUCKDB_API void BaseAppender::Append(double value);
template <>
DUCKDB_API void BaseAppender::Append(date_t value);
template <>
DUCKDB_API void BaseAppender::Append(dtime_t value);
template <>
DUCKDB_API void BaseAppender::Append(timestamp_t value);
template <>
DUCKDB_API void BaseAppender::Append(interval_t value);
template <>
DUCKDB_API void BaseAppender::Append(const char *value);
template <>
DUCKDB_API void BaseAppender::Append(string_t value);
template <>
DUCKDB_API void BaseAppender::Append(Value value);
template <>
DUCKDB_API void BaseAppender::Append(std::nullptr_t value);

} // namespace duckdb
