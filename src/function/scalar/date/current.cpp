#include "duckdb/function/scalar/date_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

static timestamp_t GetTransactionTimestamp(ExpressionState &state) {
	return state.GetContext().ActiveTransaction().start_timestamp;
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

void CurrentTimeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction current_time({}, LogicalType::TIME, CurrentTimeFunction);
	;
	current_time.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	set.AddFunction(current_time);
}

void CurrentDateFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction current_date({}, LogicalType::DATE, CurrentDateFunction);
	;
	current_date.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	set.AddFunction({"today", "current_date"}, current_date);
}

void CurrentTimestampFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction current_timestamp({}, LogicalType::TIMESTAMP_TZ, CurrentTimestampFunction);
	current_timestamp.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	set.AddFunction({"now", "get_current_timestamp", "transaction_timestamp"}, current_timestamp);
}

} // namespace duckdb
