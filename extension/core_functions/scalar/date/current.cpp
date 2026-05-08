#include "core_functions/scalar/date_functions.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
class DataChunk;

static timestamp_t GetTransactionTimestamp(ExpressionState &state) {
	return MetaTransaction::Get(state.GetContext()).start_timestamp;
}

static void CurrentTimestampFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);
	auto ts = GetTransactionTimestamp(state);
	auto val = Value::TIMESTAMPTZ(timestamp_tz_t(ts));
	result.Reference(val, count_t(input.size()));
}

ScalarFunction GetCurrentTimestampFun::GetFunction() {
	ScalarFunction current_timestamp({}, LogicalType::TIMESTAMP_TZ, CurrentTimestampFunction);
	current_timestamp.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	return current_timestamp;
}

} // namespace duckdb
