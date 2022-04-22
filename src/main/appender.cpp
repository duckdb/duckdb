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

BaseAppender::BaseAppender() : column(0) {
}

BaseAppender::BaseAppender(vector<LogicalType> types_p) : types(move(types_p)), column(0) {
	InitializeChunk();
}

BaseAppender::~BaseAppender() {
}

void BaseAppender::Destructor() {
	if (Exception::UncaughtException()) {
		return;
	}
	// flush any remaining chunks, but only if we are not cleaning up the appender as part of an exception stack unwind
	// wrapped in a try/catch because Close() can throw if the table was dropped in the meantime
	try {
		Close();
	} catch (...) {
	}
}

InternalAppender::InternalAppender(ClientContext &context_p, TableCatalogEntry &table_p)
    : BaseAppender(table_p.GetTypes()), context(context_p), table(table_p) {
}

InternalAppender::~InternalAppender() {
	Destructor();
}

Appender::Appender(Connection &con, const string &schema_name, const string &table_name)
    : BaseAppender(), context(con.context) {
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
	Destructor();
}

void BaseAppender::InitializeChunk() {
	chunk = make_unique<DataChunk>();
	chunk->Initialize(types);
}

void BaseAppender::BeginRow() {
}

void BaseAppender::EndRow() {
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
void BaseAppender::AppendValueInternal(Vector &col, SRC input) {
	FlatVector::GetData<DST>(col)[chunk->size()] = Cast::Operation<SRC, DST>(input);
}

template <class T>
void BaseAppender::AppendValueInternal(T input) {
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
void BaseAppender::Append(bool value) {
	AppendValueInternal<bool>(value);
}

template <>
void BaseAppender::Append(int8_t value) {
	AppendValueInternal<int8_t>(value);
}

template <>
void BaseAppender::Append(int16_t value) {
	AppendValueInternal<int16_t>(value);
}

template <>
void BaseAppender::Append(int32_t value) {
	AppendValueInternal<int32_t>(value);
}

template <>
void BaseAppender::Append(int64_t value) {
	AppendValueInternal<int64_t>(value);
}

template <>
void BaseAppender::Append(hugeint_t value) {
	AppendValueInternal<hugeint_t>(value);
}

template <>
void BaseAppender::Append(uint8_t value) {
	AppendValueInternal<uint8_t>(value);
}

template <>
void BaseAppender::Append(uint16_t value) {
	AppendValueInternal<uint16_t>(value);
}

template <>
void BaseAppender::Append(uint32_t value) {
	AppendValueInternal<uint32_t>(value);
}

template <>
void BaseAppender::Append(uint64_t value) {
	AppendValueInternal<uint64_t>(value);
}

template <>
void BaseAppender::Append(const char *value) {
	AppendValueInternal<string_t>(string_t(value));
}

void BaseAppender::Append(const char *value, uint32_t length) {
	AppendValueInternal<string_t>(string_t(value, length));
}

template <>
void BaseAppender::Append(string_t value) {
	AppendValueInternal<string_t>(value);
}

template <>
void BaseAppender::Append(float value) {
	AppendValueInternal<float>(value);
}

template <>
void BaseAppender::Append(double value) {
	AppendValueInternal<double>(value);
}

template <>
void BaseAppender::Append(date_t value) {
	AppendValueInternal<int32_t>(value.days);
}

template <>
void BaseAppender::Append(dtime_t value) {
	AppendValueInternal<int64_t>(value.micros);
}

template <>
void BaseAppender::Append(timestamp_t value) {
	AppendValueInternal<int64_t>(value.value);
}

template <>
void BaseAppender::Append(interval_t value) {
	AppendValueInternal<interval_t>(value);
}

template <>
void BaseAppender::Append(Value value) { // NOLINT: template shtuff
	if (column >= chunk->ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	AppendValue(value);
}

template <>
void BaseAppender::Append(std::nullptr_t value) {
	if (column >= chunk->ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunk->data[column++];
	FlatVector::SetNull(col, chunk->size(), true);
}

void BaseAppender::AppendValue(const Value &value) {
	chunk->SetValue(column, chunk->size(), value);
	column++;
}

void BaseAppender::AppendDataChunk(DataChunk &chunk) {
	if (chunk.GetTypes() != types) {
		throw InvalidInputException("Type mismatch in Append DataChunk and the types required for appender");
	}
	collection.Append(chunk);
	if (collection.ChunkCount() >= FLUSH_COUNT) {
		Flush();
	}
}

void BaseAppender::FlushChunk() {
	if (chunk->size() == 0) {
		return;
	}
	collection.Append(move(chunk));
	InitializeChunk();
	if (collection.ChunkCount() >= FLUSH_COUNT) {
		Flush();
	}
}

void BaseAppender::Flush() {
	// check that all vectors have the same length before appending
	if (column != 0) {
		throw InvalidInputException("Failed to Flush appender: incomplete append to row!");
	}

	FlushChunk();
	if (collection.Count() == 0) {
		return;
	}
	FlushInternal(collection);

	collection.Reset();
	column = 0;
}

void Appender::FlushInternal(ChunkCollection &collection) {
	context->Append(*description, collection);
}

void InternalAppender::FlushInternal(ChunkCollection &collection) {
	for (auto &chunk : collection.Chunks()) {
		table.storage->Append(table, context, *chunk);
	}
}

void BaseAppender::Close() {
	if (column == 0 || column == types.size()) {
		Flush();
	}
}

} // namespace duckdb
