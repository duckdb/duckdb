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
    : allocator(allocator_p), types(std::move(types_p)), collection(make_uniq<ColumnDataCollection>(allocator, types)),
      column(0), appender_type(type_p), flush_count(flush_count_p) {
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

const vector<LogicalType> &BaseAppender::GetActiveTypes() const {
	if (active_types.empty()) {
		return types;
	}
	return active_types;
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
		throw CatalogException(
		    StringUtil::Format("Table \"%s.%s.%s\" could not be found", database_name, schema_name, table_name));
	}
	if (description->readonly) {
		throw InvalidInputException("Cannot append to a readonly database.");
	}

	vector<optional_ptr<const ParsedExpression>> defaults;
	for (auto &column : description->columns) {
		if (column.Generated()) {
			continue;
		}
		types.push_back(column.Type());
		defaults.push_back(column.HasDefaultValue() ? &column.DefaultValue() : nullptr);
	}

	auto binder = Binder::CreateBinder(*context);
	context->RunFunctionInTransaction([&]() {
		for (idx_t i = 0; i < types.size(); i++) {
			auto &type = types[i];
			auto &expr = defaults[i];

			if (!expr) {
				// The default value is NULL.
				default_values[i] = Value(type);
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
				default_values[i] = result_value;
			}
		}
	});

	InitializeChunk();
	collection = make_uniq<ColumnDataCollection>(allocator, GetActiveTypes());
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
	chunk.Destroy();
	chunk.Initialize(allocator, GetActiveTypes());
}

void BaseAppender::BeginRow() {
}

void BaseAppender::EndRow() {
	// Ensure that all columns have been appended to.
	if (column != chunk.ColumnCount()) {
		throw InvalidInputException("Call to EndRow before all columns have been appended to!");
	}
	column = 0;
	chunk.SetCardinality(chunk.size() + 1);
	if (chunk.size() >= STANDARD_VECTOR_SIZE) {
		FlushChunk();
	}
}

template <class SRC, class DST>
void BaseAppender::AppendValueInternal(Vector &col, SRC input) {
	FlatVector::GetData<DST>(col)[chunk.size()] = Cast::Operation<SRC, DST>(input);
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
		auto &result = FlatVector::GetData<DST>(col)[chunk.size()];
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
	if (column >= GetActiveTypes().size()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunk.data[column];
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
		FlatVector::GetData<string_t>(col)[chunk.size()] = StringCast::Operation<T>(input, col);
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
	if (column >= chunk.ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	AppendValue(value);
}

void duckdb::BaseAppender::Append(DataChunk &target, const Value &value, idx_t col, idx_t row) {
	if (col >= target.ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	if (row >= target.GetCapacity()) {
		throw InvalidInputException("Too many rows for chunk!");
	}

	if (value.type() == target.GetTypes()[col]) {
		target.SetValue(col, row, value);
	} else {
		Value new_value;
		string error_msg;
		if (value.DefaultTryCastAs(target.GetTypes()[col], new_value, &error_msg)) {
			target.SetValue(col, row, new_value);
		} else {
			throw InvalidInputException("type mismatch in Append, expected %s, got %s for column %d",
			                            target.GetTypes()[col], value.type(), col);
		}
	}
}

template <>
void BaseAppender::Append(std::nullptr_t value) {
	if (column >= chunk.ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunk.data[column++];
	FlatVector::SetNull(col, chunk.size(), true);
}

void BaseAppender::AppendValue(const Value &value) {
	chunk.SetValue(column, chunk.size(), value);
	column++;
}

void BaseAppender::AppendDataChunk(DataChunk &chunk_p) {
	auto chunk_types = chunk_p.GetTypes();
	auto &appender_types = GetActiveTypes();

	// Early-out, if types match.
	if (chunk_types == appender_types) {
		collection->Append(chunk_p);
		if (collection->Count() >= flush_count) {
			Flush();
		}
		return;
	}

	auto count = chunk_p.ColumnCount();
	if (count != appender_types.size()) {
		throw InvalidInputException("incorrect column count in AppendDataChunk, expected %d, got %d",
		                            appender_types.size(), count);
	}

	// We try to cast the chunk.
	auto size = chunk_p.size();
	DataChunk cast_chunk;
	cast_chunk.Initialize(allocator, appender_types);
	cast_chunk.SetCardinality(size);

	for (idx_t i = 0; i < count; i++) {
		if (chunk_p.data[i].GetType() == appender_types[i]) {
			cast_chunk.data[i].Reference(chunk_p.data[i]);
			continue;
		}

		string error_msg;
		auto success = VectorOperations::DefaultTryCast(chunk_p.data[i], cast_chunk.data[i], size, &error_msg);
		if (!success) {
			throw InvalidInputException("type mismatch in AppendDataChunk, expected %s, got %s for column %d",
			                            appender_types[i].ToString(), chunk_p.data[i].GetType().ToString(), i);
		}
	}

	collection->Append(cast_chunk);
	if (collection->Count() >= flush_count) {
		Flush();
	}
}

void BaseAppender::FlushChunk() {
	if (chunk.size() == 0) {
		return;
	}
	collection->Append(chunk);
	chunk.Reset();
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
	context->Append(*description, collection, &column_ids);
}

void Appender::AppendDefault() {
	auto value = GetDefaultValue(column);
	Append(value);
}

void duckdb::Appender::AppendDefault(DataChunk &chunk, idx_t col, idx_t row) {
	auto value = GetDefaultValue(col);
	Append(chunk, value, col, row);
}

Value Appender::GetDefaultValue(idx_t column) {
	auto index = column;

	if (!column_ids.empty()) {
		if (column >= column_ids.size()) {
			throw InvalidInputException("Column index out of bounds");
		}
		index = column_ids[column].index;
	}

	auto it = default_values.find(index);
	if (it == default_values.end()) {
		auto &name = description->columns[index].Name();
		throw NotImplementedException(
		    "AppendDefault is not supported for column \"%s\": not a foldable default expressions.", name);
	}
	return it->second;
}

void Appender::AddColumn(const string &name) {
	Flush();

	auto exists = false;
	for (idx_t col_idx = 0; col_idx < description->columns.size(); col_idx++) {
		auto &col_def = description->columns[col_idx];
		if (col_def.Name() != name) {
			continue;
		}

		// Ensure that we are not adding a generated column.
		if (col_def.Generated()) {
			throw InvalidInputException("cannot add a generated column to the appender");
		}

		// Ensure that we haven't added this column before.
		for (const auto &column_id : column_ids) {
			if (column_id == col_def.Logical()) {
				throw InvalidInputException("cannot add the same column twice");
			}
		}

		active_types.push_back(col_def.Type());
		column_ids.push_back(col_def.Logical());
		exists = true;
		break;
	}
	if (!exists) {
		throw InvalidInputException("the column must exist in the table");
	}

	InitializeChunk();
	collection = make_uniq<ColumnDataCollection>(allocator, GetActiveTypes());
}

void Appender::ClearColumns() {
	Flush();
	column_ids.clear();
	active_types.clear();

	InitializeChunk();
	collection = make_uniq<ColumnDataCollection>(allocator, GetActiveTypes());
}

void InternalAppender::FlushInternal(ColumnDataCollection &collection) {
	auto binder = Binder::CreateBinder(context);
	auto bound_constraints = binder->BindConstraints(table);
	table.GetStorage().LocalAppend(table, context, collection, bound_constraints, nullptr);
}

void InternalAppender::AddColumn(const string &name) {
	throw InternalException("AddColumn not implemented for InternalAppender");
}

void InternalAppender::ClearColumns() {
	throw InternalException("ClearColumns not implemented for InternalAppender");
}

void BaseAppender::Close() {
	if (column == 0 || column == GetActiveTypes().size()) {
		Flush();
	}
}

} // namespace duckdb
