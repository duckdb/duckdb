// REGR_SLOPE(y, x)
// Returns the slope of the linear regression line for non-null pairs in a group.
// It is computed for non-null pairs using the following formula:
// COVAR_POP(x,y) / VAR_POP(x)

//! Input : Any numeric type
//! Output : Double

#pragma once
#include "duckdb/function/aggregate/algebraic/stddev.hpp"
#include "duckdb/function/aggregate/algebraic/covar.hpp"

namespace duckdb {

struct RegrSlopeState {
	CovarState cov_pop;
	StddevState var_pop;
};

struct RegrSlopeOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		CovarOperation::Initialize<CovarState>(&state->cov_pop);
		STDDevBaseOperation::Initialize<StddevState>(&state->var_pop);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &aggr_input_data, A_TYPE *x_data, B_TYPE *y_data,
	                      ValidityMask &amask, ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		CovarOperation::Operation<A_TYPE, B_TYPE, CovarState, OP>(&state->cov_pop, aggr_input_data, y_data, x_data,
		                                                          bmask, amask, yidx, xidx);
		STDDevBaseOperation::Operation<A_TYPE, StddevState, OP>(&state->var_pop, aggr_input_data, y_data, bmask, yidx);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &aggr_input_data) {
		CovarOperation::Combine<CovarState, OP>(source.cov_pop, &target->cov_pop, aggr_input_data);
		STDDevBaseOperation::Combine<StddevState, OP>(source.var_pop, &target->var_pop, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (state->cov_pop.count == 0 || state->var_pop.count == 0) {
			mask.SetInvalid(idx);
		} else {
			auto cov = state->cov_pop.co_moment / state->cov_pop.count;
			auto var_pop = state->var_pop.count > 1 ? (state->var_pop.dsquared / state->var_pop.count) : 0;
			if (!Value::DoubleIsFinite(var_pop)) {
				throw OutOfRangeException("VARPOP is out of range!");
			}
			if (var_pop == 0) {
				mask.SetInvalid(idx);
				return;
			}
			target[idx] = cov / var_pop;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};
} // namespace duckdb
