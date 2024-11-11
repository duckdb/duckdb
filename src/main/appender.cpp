#include "duckdb/main/appender.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

BaseAppender::BaseAppender(Allocator &allocator, const AppenderType type_p)
    : allocator(allocator), column(0), appender_type(type_p) {
}

BaseAppender::BaseAppender(Allocator &allocator_p, vector<LogicalType> types_p, const AppenderType type_p,
                           const idx_t flush_count_p)
    : allocator(allocator_p), table_types(std::move(types_p)),
      collection(make_uniq<ColumnDataCollection>(allocator, table_types)), column(0), appender_type(type_p),
      flush_count(flush_count_p) {
	InitializeChunk();
}

BaseAppender::~BaseAppender() {
}

void BaseAppender::Destructor() {
	if (Exception::UncaughtException()) {
		return;
	}
	// Flush any remaining chunks, if we are not cleaning up as part of an exception stack unwind wrapped in a
	// try/catch. Close() can throw if the table was dropped in the meantime.
	try {
		Close();
	} catch (...) { // NOLINT
	}
}

InternalAppender::InternalAppender(ClientContext &context_p, TableCatalogEntry &table_p, const idx_t flush_count_p)
    : BaseAppender(Allocator::DefaultAllocator(), table_p.GetTypes(), AppenderType::PHYSICAL, flush_count_p),
      context(context_p), table(table_p) {
}

InternalAppender::~InternalAppender() {
	Destructor();
}

Appender::Appender(Connection &con, const string &database_name, const string &schema_name, const string &table_name)
    : BaseAppender(Allocator::DefaultAllocator(), AppenderType::LOGICAL), context(con.context) {

	description = con.TableInfo(database_name, schema_name, table_name);
	if (!description) {
		throw CatalogException(StringUtil::Format("Table \"%s.%s\" could not be found", schema_name, table_name));
	}
	if (description->readonly) {
		throw InvalidInputException("Cannot append to a readonly database.");
	}

	vector<optional_ptr<const ParsedExpression>> defaults;
	for (auto &column : description->columns) {
		if (column.Generated()) {
			continue;
		}
		table_types.push_back(column.Type());
		defaults.push_back(column.HasDefaultValue() ? &column.DefaultValue() : nullptr);
	}

	auto binder = Binder::CreateBinder(*context);
	context->RunFunctionInTransaction([&]() {
		for (idx_t i = 0; i < table_types.size(); i++) {
			auto &type = table_types[i];
			auto &expr = defaults[i];

			if (!expr) {
				// The default value is NULL.
				table_default_values[i] = Value(type);
				continue;
			}

			auto default_copy = expr->Copy();
			D_ASSERT(!default_copy->HasParameter());

			ConstantBinder default_binder(*binder, *context, "DEFAULT value");
			default_binder.target_type = type;
			auto bound_default = default_binder.Bind(default_copy);

			if (!bound_default->IsFoldable()) {
				// Not supported yet.
				continue;
			}

			Value result_value;
			auto eval_success = ExpressionExecutor::TryEvaluateScalar(*context, *bound_default, result_value);
			// Insert the default Value.
			if (eval_success) {
				table_default_values[i] = result_value;
			}
		}
	});

	active_types = table_types;
	active_default_values = table_default_values;
	InitializeChunk();
	collection = make_uniq<ColumnDataCollection>(allocator, active_types);
}

Appender::Appender(Connection &con, const string &schema_name, const string &table_name)
    : Appender(con, INVALID_CATALOG, schema_name, table_name) {
}

Appender::Appender(Connection &con, const string &table_name)
    : Appender(con, INVALID_CATALOG, DEFAULT_SCHEMA, table_name) {
}

Appender::~Appender() {
	Destructor();
}

void BaseAppender::InitializeChunk() {
	chunkk.Initialize(allocator, active_types);
}

void BaseAppender::BeginRow() {
}

void BaseAppender::EndRow() {
	// Ensure that all columns have been appended to.
	if (column != chunkk.ColumnCount()) {
		throw InvalidInputException("Call to EndRow before all columns have been appended to!");
	}
	column = 0;
	chunkk.SetCardinality(chunkk.size() + 1);
	if (chunkk.size() >= STANDARD_VECTOR_SIZE) {
		FlushChunk();
	}
}

template <class SRC, class DST>
void BaseAppender::AppendValueInternal(Vector &col, SRC input) {
	FlatVector::GetData<DST>(col)[chunkk.size()] = Cast::Operation<SRC, DST>(input);
}

template <class SRC, class DST>
void BaseAppender::AppendDecimalValueInternal(Vector &col, SRC input) {
	switch (appender_type) {
	case AppenderType::LOGICAL: {
		auto &type = col.GetType();
		D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		CastParameters parameters;
		auto &result = FlatVector::GetData<DST>(col)[chunkk.size()];
		TryCastToDecimal::Operation<SRC, DST>(input, result, parameters, width, scale);
		return;
	}
	case AppenderType::PHYSICAL: {
		AppendValueInternal<SRC, DST>(col, input);
		return;
	}
	default:
		throw InternalException("Type not implemented for AppenderType");
	}
}

template <class T>
void BaseAppender::AppendValueInternal(T input) {
	if (column >= active_types.size()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunkk.data[column];
	switch (col.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		AppendValueInternal<T, bool>(col, input);
		break;
	case LogicalTypeId::UTINYINT:
		AppendValueInternal<T, uint8_t>(col, input);
		break;
	case LogicalTypeId::TINYINT:
		AppendValueInternal<T, int8_t>(col, input);
		break;
	case LogicalTypeId::USMALLINT:
		AppendValueInternal<T, uint16_t>(col, input);
		break;
	case LogicalTypeId::SMALLINT:
		AppendValueInternal<T, int16_t>(col, input);
		break;
	case LogicalTypeId::UINTEGER:
		AppendValueInternal<T, uint32_t>(col, input);
		break;
	case LogicalTypeId::INTEGER:
		AppendValueInternal<T, int32_t>(col, input);
		break;
	case LogicalTypeId::UBIGINT:
		AppendValueInternal<T, uint64_t>(col, input);
		break;
	case LogicalTypeId::BIGINT:
		AppendValueInternal<T, int64_t>(col, input);
		break;
	case LogicalTypeId::HUGEINT:
		AppendValueInternal<T, hugeint_t>(col, input);
		break;
	case LogicalTypeId::UHUGEINT:
		AppendValueInternal<T, uhugeint_t>(col, input);
		break;
	case LogicalTypeId::FLOAT:
		AppendValueInternal<T, float>(col, input);
		break;
	case LogicalTypeId::DOUBLE:
		AppendValueInternal<T, double>(col, input);
		break;
	case LogicalTypeId::DECIMAL:
		switch (col.GetType().InternalType()) {
		case PhysicalType::INT16:
			AppendDecimalValueInternal<T, int16_t>(col, input);
			break;
		case PhysicalType::INT32:
			AppendDecimalValueInternal<T, int32_t>(col, input);
			break;
		case PhysicalType::INT64:
			AppendDecimalValueInternal<T, int64_t>(col, input);
			break;
		case PhysicalType::INT128:
			AppendDecimalValueInternal<T, hugeint_t>(col, input);
			break;
		default:
			throw InternalException("Internal type not recognized for Decimal");
		}
		break;
	case LogicalTypeId::DATE:
		AppendValueInternal<T, date_t>(col, input);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		AppendValueInternal<T, timestamp_t>(col, input);
		break;
	case LogicalTypeId::TIME:
		AppendValueInternal<T, dtime_t>(col, input);
		break;
	case LogicalTypeId::TIME_TZ:
		AppendValueInternal<T, dtime_tz_t>(col, input);
		break;
	case LogicalTypeId::INTERVAL:
		AppendValueInternal<T, interval_t>(col, input);
		break;
	case LogicalTypeId::VARCHAR:
		FlatVector::GetData<string_t>(col)[chunkk.size()] = StringCast::Operation<T>(input, col);
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
void BaseAppender::Append(uhugeint_t value) {
	AppendValueInternal<uhugeint_t>(value);
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
	AppendValueInternal<date_t>(value);
}

template <>
void BaseAppender::Append(dtime_t value) {
	AppendValueInternal<dtime_t>(value);
}

template <>
void BaseAppender::Append(timestamp_t value) {
	AppendValueInternal<timestamp_t>(value);
}

template <>
void BaseAppender::Append(interval_t value) {
	AppendValueInternal<interval_t>(value);
}

template <>
void BaseAppender::Append(Value value) { // NOLINT: template stuff
	if (column >= chunkk.ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	AppendValue(value);
}

template <>
void BaseAppender::Append(std::nullptr_t value) {
	if (column >= chunkk.ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunkk.data[column++];
	FlatVector::SetNull(col, chunkk.size(), true);
}

void BaseAppender::AppendValue(const Value &value) {
	chunkk.SetValue(column, chunkk.size(), value);
	column++;
}

void BaseAppender::AppendDataChunk(DataChunk &chunk_p) {
	auto chunk_types = chunk_p.GetTypes();

	// Early-out, if types match.
	if (chunk_types == active_types) {
		collection->Append(chunk_p);
		if (collection->Count() >= flush_count) {
			Flush();
		}
		return;
	}

	auto count = chunk_p.ColumnCount();
	if (count != active_types.size()) {
		throw InvalidInputException("incorrect column count in AppendDataChunk, expected %d, got %d",
		                            active_types.size(), count);
	}

	// We try to cast the chunk.
	auto size = chunk_p.size();
	DataChunk cast_chunk;
	cast_chunk.Initialize(allocator, active_types);
	cast_chunk.SetCardinality(size);

	for (idx_t i = 0; i < count; i++) {
		if (chunk_p.data[i].GetType() == active_types[i]) {
			cast_chunk.data[i].Reference(chunk_p.data[i]);
			continue;
		}

		string error_msg;
		auto success = VectorOperations::DefaultTryCast(chunk_p.data[i], cast_chunk.data[i], size, &error_msg);
		if (!success) {
			throw InvalidInputException("type mismatch in AppendDataChunk, expected %s, got %s for column %d",
			                            active_types[i].ToString(), chunk_p.data[i].GetType().ToString(), i);
		}
	}

	collection->Append(cast_chunk);
	if (collection->Count() >= flush_count) {
		Flush();
	}
}

void BaseAppender::FlushChunk() {
	if (chunkk.size() == 0) {
		return;
	}
	collection->Append(chunkk);
	chunkk.Reset();
	if (collection->Count() >= flush_count) {
		Flush();
	}
}

void BaseAppender::Flush() {
	// Check that all vectors have the same length before appending.
	if (column != 0) {
		throw InvalidInputException("Failed to Flush appender: incomplete append to row!");
	}

	FlushChunk();
	if (collection->Count() == 0) {
		return;
	}

	FlushInternal(*collection);
	collection->Reset();
	column = 0;
}

void Appender::FlushInternal(ColumnDataCollection &collection) {
	context->Append(*description, collection, &active_columns);
}

void Appender::AppendDefault() {
	auto it = active_default_values.find(column);
	if (it == active_default_values.end()) {
		throw NotImplementedException(
		    "AppendDefault is currently not supported for column \"%s\" because default expression is not foldable.",
		    active_columns[column]);
	}
	auto &default_value = it->second;
	Append(default_value);
}

void Appender::AddColumn(const string &name) {
	Flush();
	auto exists = false;
	for (const auto &col_def : description->columns) {
		if (col_def.Name() == name) {
			if (col_def.Generated()) {
				throw InvalidInputException("cannot add a generated column to the appender");
			}
			exists = true;
			break;
		}
	}
	if (!exists) {
		throw InvalidInputException("the column must exist in the table");
	}

	active_columns.push_back(name);
	active_types.clear();
	active_default_values.clear();

	for (idx_t active_idx = 0; active_idx < active_columns.size(); active_idx++) {
		for (idx_t col_idx = 0; col_idx < description->columns.size(); col_idx++) {
			auto &col_def = description->columns[col_idx];
			if (col_def.Name() == active_columns[active_idx]) {
				active_types.push_back(col_def.Type());
				active_default_values.insert({active_idx, table_default_values[col_idx]});
			}
		}
	}

	chunkk.Destroy();
	InitializeChunk();
	collection = make_uniq<ColumnDataCollection>(allocator, active_types);
}

void Appender::ClearColumns() {
	Flush();
	active_columns.clear();
	active_types = table_types;
	active_default_values = table_default_values;

	chunkk.Destroy();
	InitializeChunk();
	collection = make_uniq<ColumnDataCollection>(allocator, active_types);
}

void InternalAppender::FlushInternal(ColumnDataCollection &collection) {
	auto binder = Binder::CreateBinder(context);
	auto bound_constraints = binder->BindConstraints(table);
	table.GetStorage().LocalAppend(table, context, collection, bound_constraints);
}

void InternalAppender::AddColumn(const string &name) {
	throw InternalException("AddColumn not implemented for InternalAppender");
}

void InternalAppender::ClearColumns() {
	throw InternalException("ClearColumns not implemented for InternalAppender");
}

void BaseAppender::Close() {
	if (column == 0 || column == active_types.size()) {
		Flush();
	}
}

} // namespace duckdb
