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
#include "duckdb/common/types/selection_vector.hpp"

namespace duckdb {

BaseAppender::BaseAppender(Allocator &allocator, AppenderType type_p)
    : allocator(allocator), column(0), appender_type(type_p) {
}

BaseAppender::BaseAppender(Allocator &allocator_p, vector<LogicalType> types_p, AppenderType type_p)
    : allocator(allocator_p), types(std::move(types_p)), collection(make_uniq<ColumnDataCollection>(allocator, types)),
      column(0), appender_type(type_p) {
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
	} catch (...) { // NOLINT
	}
}

InternalAppender::InternalAppender(ClientContext &context_p, TableCatalogEntry &table_p)
    : BaseAppender(Allocator::DefaultAllocator(), table_p.GetTypes(), AppenderType::PHYSICAL), context(context_p),
      table(table_p) {
}

InternalAppender::~InternalAppender() {
	Destructor();
}

Appender::Appender(Connection &con, const string &schema_name, const string &table_name)
    : BaseAppender(Allocator::DefaultAllocator(), AppenderType::LOGICAL), context(con.context) {
	description = con.TableInfo(schema_name, table_name);
	if (!description) {
		// table could not be found
		throw CatalogException(StringUtil::Format("Table \"%s.%s\" could not be found", schema_name, table_name));
	}
	vector<optional_ptr<const ParsedExpression>> defaults;
	for (auto &column : description->columns) {
		types.push_back(column.Type());
		defaults.push_back(column.HasDefaultValue() ? &column.DefaultValue() : nullptr);
	}
	auto binder = Binder::CreateBinder(*context);

	context->RunFunctionInTransaction([&]() {
		for (idx_t i = 0; i < types.size(); i++) {
			auto &type = types[i];
			auto &expr = defaults[i];

			if (!expr) {
				// Insert NULL
				bound_defaults.push_back(make_uniq<BoundConstantExpression>(Value(type)));
				continue;
			}
			auto default_copy = expr->Copy();
			D_ASSERT(!default_copy->HasParameter());
			ConstantBinder default_binder(*binder, *context, "DEFAULT value");
			default_binder.target_type = type;
			auto bound_default = default_binder.Bind(default_copy);
			Value result_value;
			if (bound_default->IsFoldable() &&
			    ExpressionExecutor::TryEvaluateScalar(*context, *bound_default, result_value)) {
				// Insert the evaluated Value
				bound_defaults.push_back(make_uniq<BoundConstantExpression>(result_value));
			} else {
				// Insert a bound Expression
				bound_defaults.push_back(std::move(bound_default));
			}
		}
	});

	expression_executor = make_uniq<ExpressionExecutor>(*context, bound_defaults);
	InitializeChunk();
	collection = make_uniq<ColumnDataCollection>(allocator, types);
}

Appender::Appender(Connection &con, const string &table_name) : Appender(con, DEFAULT_SCHEMA, table_name) {
}

Appender::~Appender() {
	Destructor();
}

void BaseAppender::InitializeChunk() {
	chunk.Initialize(allocator, types);
}

void BaseAppender::BeginRow() {
}

void BaseAppender::EndRow() {
	// check that all rows have been appended to
	if (column != chunk.ColumnCount()) {
		throw InvalidInputException("Call to EndRow before all rows have been appended to!");
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
		TryCastToDecimal::Operation<SRC, DST>(input, FlatVector::GetData<DST>(col)[chunk.size()], parameters, width,
		                                      scale);
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
	if (column >= types.size()) {
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
void BaseAppender::Append(Value value) { // NOLINT: template shtuff
	if (column >= chunk.ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	AppendValue(value);
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

void BaseAppender::AppendDataChunk(DataChunk &chunk) {
	auto chunk_types = chunk.GetTypes();
	if (chunk_types != types) {
		for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
			if (chunk.data[i].GetType() != types[i]) {
				throw InvalidInputException("Type mismatch in Append DataChunk and the types required for appender, "
				                            "expected %s but got %s for column %d",
				                            types[i].ToString(), chunk.data[i].GetType().ToString(), i + 1);
			}
		}
	}
	collection->Append(chunk);
	if (collection->Count() >= FLUSH_COUNT) {
		Flush();
	}
}

void BaseAppender::FlushChunk() {
	if (chunk.size() == 0) {
		return;
	}
	collection->Append(chunk);
	chunk.Reset();
	if (collection->Count() >= FLUSH_COUNT) {
		Flush();
	}
}

void BaseAppender::Flush() {
	// check that all vectors have the same length before appending
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
	context->Append(*description, collection);
}

void Appender::AppendDefaultsToVector(Vector &result, idx_t column, SelectionVector &sel, idx_t count) {
#ifdef DEBUG
	for (idx_t i = 0; i < count; i++) {
		auto index = sel.get_index(i);
		if (index > STANDARD_VECTOR_SIZE) {
			throw InternalException("Provided SelectionVector is invalid, index %d points to %d, which is out of range",
			                        i, index);
		}
	}
#endif
	if (count > STANDARD_VECTOR_SIZE) {
		throw InvalidInputException("Please provide a Vector that is at most STANDARD_VECTOR_SIZE (%d) in size",
		                            STANDARD_VECTOR_SIZE);
	}
	if (column >= types.size()) {
		throw InvalidInputException(
		    "Could not append DEFAULT value of column %d, out of range index, table only has %d columns", column,
		    types.size());
	}
	auto &type = types[column];
	if (result.GetType() != type) {
		throw InvalidInputException("Type mismatch, column has type: %s, while the provided Vector has type %s",
		                            type.ToString(), result.GetType().ToString());
	}

	auto &executor = *expression_executor;

	// This is only used to tell the ExpressionExecutor the size of our result
	DataChunk empty_chunk;
	empty_chunk.SetCardinality(count);
	executor.SetChunk(empty_chunk);

	// FIXME: if we are feeding the Vector to the ExecuteExpression method, we have to first make sure
	// that the validity mask of the Vector is clean
	context->RunFunctionInTransaction([&]() { executor.ExecuteExpression(column, result, sel); });
}

void Appender::AppendDefault() {
	auto &default_expr = bound_defaults[column];
	if (default_expr->type == ExpressionType::VALUE_CONSTANT) {
		// Fast path: NULL or constant-folded
		auto &bound_constant = default_expr->Cast<BoundConstantExpression>();
		Append(bound_constant.value);
		return;
	}

	auto &type = types[column];
	Vector result(type, 1);
	auto &executor = *expression_executor;
	// The executor is initialized with expressions for every column, even though only some are used
	// this makes it so that we can just use the 'column' index

	// FIXME: we could optimize this further by delaying this until Flush is called, then doing it for the entire chunk
	// once
	context->RunFunctionInTransaction([&]() { executor.ExecuteExpression(column, result); });
	Append(result.GetValue(0));
}

void InternalAppender::FlushInternal(ColumnDataCollection &collection) {
	auto binder = Binder::CreateBinder(context);
	auto bound_constraints = binder->BindConstraints(table);
	table.GetStorage().LocalAppend(table, context, collection, bound_constraints);
}

void BaseAppender::Close() {
	if (column == 0 || column == types.size()) {
		Flush();
	}
}

} // namespace duckdb
