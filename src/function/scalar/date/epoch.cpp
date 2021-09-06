#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

struct EpochSecOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		return Timestamp::FromEpochSeconds(input);
	}
};

static void EpochSecFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);

	UnaryExecutor::Execute<int64_t, timestamp_t, EpochSecOperator>(input.data[0], result, input.size());
}

struct EpochMillisOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		return Timestamp::FromEpochMs(input);
	}
};

static void EpochMillisFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);

	UnaryExecutor::Execute<int64_t, timestamp_t, EpochMillisOperator>(input.data[0], result, input.size());
}

void EpochFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet epoch("epoch_ms");
	epoch.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::TIMESTAMP, EpochMillisFunction));
	set.AddFunction(epoch);
	// to_timestamp is an alias from Postgres that converts the time in seconds to a timestamp
	ScalarFunctionSet to_timestamp("to_timestamp");
	to_timestamp.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::TIMESTAMP, EpochSecFunction));
	set.AddFunction(to_timestamp);
}

} // namespace duckdb
