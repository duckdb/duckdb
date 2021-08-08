#include "duckdb/main/appender.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"

namespace duckdb {

Appender::Appender(Connection &con, const string &schema_name, const string &table_name)
    : context(con.context), column(0) {
	description = con.TableInfo(schema_name, table_name);
	if (!description) {
		// table could not be found
		throw CatalogException(StringUtil::Format("Table \"%s.%s\" could not be found", schema_name, table_name));
	}
	for (auto &column : description->columns) {
		types.push_back(column.type);
	}
	InitializeChunk();
}

Appender::Appender(Connection &con, const string &table_name) : Appender(con, DEFAULT_SCHEMA, table_name) {
}

Appender::~Appender() {
	// flush any remaining chunks
	// wrapped in a try/catch because Close() can throw if the table was dropped in the meantime
	try {
		Close();
	} catch (...) {
	}
}

void Appender::InitializeChunk() {
	chunk = make_unique<DataChunk>();
	chunk->Initialize(types);
}

void Appender::BeginRow() {
}

void Appender::EndRow() {
	// check that all rows have been appended to
	if (column != chunk->ColumnCount()) {
		throw InvalidInputException("Call to EndRow before all rows have been appended to!");
	}
	column = 0;
	chunk->SetCardinality(chunk->size() + 1);
	if (chunk->size() >= STANDARD_VECTOR_SIZE) {
		FlushChunk();
	}
}

template <class SRC, class DST>
void Appender::AppendValueInternal(Vector &col, SRC input) {
	FlatVector::GetData<DST>(col)[chunk->size()] = Cast::Operation<SRC, DST>(input);
}

template <class T>
void Appender::AppendValueInternal(T input) {
	if (column >= types.size()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunk->data[column];
	switch (col.GetType().InternalType()) {
	case PhysicalType::BOOL:
		AppendValueInternal<T, bool>(col, input);
		break;
	case PhysicalType::UINT8:
		AppendValueInternal<T, uint8_t>(col, input);
		break;
	case PhysicalType::INT8:
		AppendValueInternal<T, int8_t>(col, input);
		break;
	case PhysicalType::UINT16:
		AppendValueInternal<T, uint16_t>(col, input);
		break;
	case PhysicalType::INT16:
		AppendValueInternal<T, int16_t>(col, input);
		break;
	case PhysicalType::UINT32:
		AppendValueInternal<T, uint32_t>(col, input);
		break;
	case PhysicalType::INT32:
		AppendValueInternal<T, int32_t>(col, input);
		break;
	case PhysicalType::UINT64:
		AppendValueInternal<T, uint64_t>(col, input);
		break;
	case PhysicalType::INT64:
		AppendValueInternal<T, int64_t>(col, input);
		break;
	case PhysicalType::INT128:
		AppendValueInternal<T, hugeint_t>(col, input);
		break;
	case PhysicalType::FLOAT:
		AppendValueInternal<T, float>(col, input);
		break;
	case PhysicalType::DOUBLE:
		AppendValueInternal<T, double>(col, input);
		break;
	case PhysicalType::VARCHAR:
		FlatVector::GetData<string_t>(col)[chunk->size()] = StringCast::Operation<T>(input, col);
		break;
	default:
		AppendValue(Value::CreateValue<T>(input));
		return;
	}
	column++;
}

template <>
void Appender::Append(bool value) {
	AppendValueInternal<bool>(value);
}

template <>
void Appender::Append(int8_t value) {
	AppendValueInternal<int8_t>(value);
}

template <>
void Appender::Append(int16_t value) {
	AppendValueInternal<int16_t>(value);
}

template <>
void Appender::Append(int32_t value) {
	AppendValueInternal<int32_t>(value);
}

template <>
void Appender::Append(int64_t value) {
	AppendValueInternal<int64_t>(value);
}

template <>
void Appender::Append(hugeint_t value) {
	AppendValueInternal<hugeint_t>(value);
}

template <>
void Appender::Append(uint8_t value) {
	AppendValueInternal<uint8_t>(value);
}

template <>
void Appender::Append(uint16_t value) {
	AppendValueInternal<uint16_t>(value);
}

template <>
void Appender::Append(uint32_t value) {
	AppendValueInternal<uint32_t>(value);
}

template <>
void Appender::Append(uint64_t value) {
	AppendValueInternal<uint64_t>(value);
}

template <>
void Appender::Append(const char *value) {
	AppendValueInternal<string_t>(string_t(value));
}

void Appender::Append(const char *value, uint32_t length) {
	AppendValueInternal<string_t>(string_t(value, length));
}

template <>
void Appender::Append(string_t value) {
	AppendValueInternal<string_t>(value);
}

template <>
void Appender::Append(float value) {
	if (!Value::FloatIsValid(value)) {
		throw InvalidInputException("Float value is out of range!");
	}
	AppendValueInternal<float>(value);
}

template <>
void Appender::Append(double value) {
	if (!Value::DoubleIsValid(value)) {
		throw InvalidInputException("Double value is out of range!");
	}
	AppendValueInternal<double>(value);
}

template <>
void Appender::Append(date_t value) {
	AppendValueInternal<int32_t>(value.days);
}

template <>
void Appender::Append(dtime_t value) {
	AppendValueInternal<int64_t>(value.micros);
}

template <>
void Appender::Append(timestamp_t value) {
	AppendValueInternal<int64_t>(value.value);
}

template <>
void Appender::Append(interval_t value) {
	AppendValueInternal<interval_t>(value);
}

template <>
void Appender::Append(Value value) { // NOLINT: template shtuff
	if (column >= chunk->ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	AppendValue(value);
}

template <>
void Appender::Append(std::nullptr_t value) {
	if (column >= chunk->ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunk->data[column++];
	FlatVector::SetNull(col, chunk->size(), true);
}

void Appender::AppendValue(const Value &value) {
	chunk->SetValue(column, chunk->size(), value);
	column++;
}

void Appender::FlushChunk() {
	if (chunk->size() == 0) {
		return;
	}
	collection.Append(move(chunk));
	InitializeChunk();
	if (collection.ChunkCount() >= FLUSH_COUNT) {
		Flush();
	}
}

void Appender::Flush() {
	// check that all vectors have the same length before appending
	if (column != 0) {
		throw InvalidInputException("Failed to Flush appender: incomplete append to row!");
	}

	FlushChunk();
	if (collection.Count() == 0) {
		return;
	}
	context->Append(*description, collection);

	collection.Reset();
	column = 0;
}

void Appender::Close() {
	if (column == 0 || column == types.size()) {
		Flush();
	}
}

} // namespace duckdb
