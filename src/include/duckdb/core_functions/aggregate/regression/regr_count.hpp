//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/regression/regr_count.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/core_functions/aggregate/algebraic/covar.hpp"
#include "duckdb/core_functions/aggregate/algebraic/stddev.hpp"

namespace duckdb {

struct RegrCountFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		*state = 0;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		*target += source;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		target[idx] = *state;
	}
	static bool IgnoreNull() {
		return true;
	}
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, A_TYPE *x_data, B_TYPE *y_data, ValidityMask &amask,
	                      ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		*state += 1;
	}
};

} // namespace duckdb
