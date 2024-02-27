//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/regression/regr_count.hpp
//
//
//===----------------------------------------------------------------------===//
// REGR_COUNT(y, x)

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/core_functions/aggregate/algebraic/covar.hpp"
#include "duckdb/core_functions/aggregate/algebraic/stddev.hpp"

namespace duckdb {

struct RegrCountFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state = 0;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target += source;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		target = static_cast<T>(state);
	}
	static bool IgnoreNull() {
		return true;
	}
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &, const B_TYPE &, AggregateBinaryInput &) {
		state += 1;
	}
};

} // namespace duckdb
