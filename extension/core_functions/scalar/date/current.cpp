#include "core_functions/scalar/date_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

static timestamp_t GetTransactionTimestamp(ExpressionState &state) {
	return MetaTransaction::Get(state.GetContext()).start_timestamp;
}

static void CurrentTimestampFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);
	auto ts = GetTransactionTimestamp(state);
	auto val = Value::TIMESTAMPTZ(timestamp_tz_t(ts));
	result.Reference(val);
}

ScalarFunction GetCurrentTimestampFun::GetFunction() {
	ScalarFunction current_timestamp({}, LogicalType::TIMESTAMP_TZ, CurrentTimestampFunction);
	current_timestamp.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	return current_timestamp;
}

} // namespace duckdb
