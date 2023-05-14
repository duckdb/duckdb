#include "duckdb/core_functions/scalar/date_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

static timestamp_t GetTransactionTimestamp(ExpressionState &state) {
	return MetaTransaction::Get(state.GetContext()).start_timestamp;
}

static void CurrentTimeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);
	auto val = Value::TIME(Timestamp::GetTime(GetTransactionTimestamp(state)));
	result.Reference(val);
}

static void CurrentDateFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);

	auto val = Value::DATE(Timestamp::GetDate(GetTransactionTimestamp(state)));
	result.Reference(val);
}

static void CurrentTimestampFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);

	auto val = Value::TIMESTAMPTZ(GetTransactionTimestamp(state));
	result.Reference(val);
}

ScalarFunction CurrentTimeFun::GetFunction() {
	ScalarFunction current_time({}, LogicalType::TIME, CurrentTimeFunction);
	current_time.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return current_time;
}

ScalarFunction CurrentDateFun::GetFunction() {
	ScalarFunction current_date({}, LogicalType::DATE, CurrentDateFunction);
	current_date.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return current_date;
}

ScalarFunction GetCurrentTimestampFun::GetFunction() {
	ScalarFunction current_timestamp({}, LogicalType::TIMESTAMP_TZ, CurrentTimestampFunction);
	current_timestamp.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return current_timestamp;
}

} // namespace duckdb
