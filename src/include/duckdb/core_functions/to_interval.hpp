//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/to_interval.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/common/operator/multiply.hpp"

namespace duckdb {

struct ToSecondsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = 0;
		if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(input, Interval::MICROS_PER_SEC,
		                                                               result.micros)) {
			throw OutOfRangeException("Interval value %d seconds out of range", input);
		}
		return result;
	}
};

} // namespace duckdb
