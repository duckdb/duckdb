#include "core_functions/scalar/date_functions.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

struct EpochSecOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE sec) {
		int64_t result;
		if (!TryCast::Operation(sec * Interval::MICROS_PER_SEC, result)) {
			throw ConversionException("Epoch seconds out of range for TIMESTAMP WITH TIME ZONE");
		}
		return timestamp_t(result);
	}
};

static void EpochSecFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);

	UnaryExecutor::Execute<double, timestamp_t, EpochSecOperator>(input.data[0], result, input.size());
}

ScalarFunction ToTimestampFun::GetFunction() {
	// to_timestamp is an alias from Postgres that converts the time in seconds to a timestamp
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::TIMESTAMP_TZ, EpochSecFunction);
}

struct NormalizedIntervalOperator {
	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		return input.Normalize();
	}
};

static void NormalizedIntervalFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);

	UnaryExecutor::Execute<interval_t, interval_t, NormalizedIntervalOperator>(input.data[0], result, input.size());
}

ScalarFunction NormalizedIntervalFun::GetFunction() {
	return ScalarFunction({LogicalType::INTERVAL}, LogicalType::INTERVAL, NormalizedIntervalFunction);
}

struct TimeTZSortKeyOperator {
	template <typename INPUT_TYPE, typename RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		return input.sort_key();
	}
};

static void TimeTZSortKeyFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);

	UnaryExecutor::Execute<dtime_tz_t, uint64_t, TimeTZSortKeyOperator>(input.data[0], result, input.size());
}

ScalarFunction TimeTZSortKeyFun::GetFunction() {
	return ScalarFunction({LogicalType::TIME_TZ}, LogicalType::UBIGINT, TimeTZSortKeyFunction);
}
} // namespace duckdb
