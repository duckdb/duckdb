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
struct regr_slope_state_t {
	covar_state_t cov_pop;
	stddev_state_t var_pop;
};

struct RegrSlopeOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		CovarOperation::Initialize<covar_state_t>(&state->cov_pop);
		STDDevBaseOperation::Initialize<stddev_state_t>(&state->var_pop);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask,
	                      nullmask_t &bnullmask, idx_t xidx, idx_t yidx) {
		CovarOperation::Operation<A_TYPE, B_TYPE, covar_state_t, OP>(&state->cov_pop, bind_data, y_data, x_data,
		                                                             bnullmask, anullmask, yidx, xidx);
		STDDevBaseOperation::Operation<A_TYPE, stddev_state_t, OP>(&state->var_pop, bind_data, y_data, bnullmask, yidx);
	}

	template <class STATE, class OP>
	static void Combine(STATE source, STATE *target) {
		CovarOperation::Combine<covar_state_t, OP>(source.cov_pop, &target->cov_pop);
		STDDevBaseOperation::Combine<stddev_state_t, OP>(source.var_pop, &target->var_pop);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (state->cov_pop.count == 0 || state->var_pop.count == 0) {
			nullmask[idx] = true;
		} else {
			auto cov = state->cov_pop.co_moment / state->cov_pop.count;
			auto var_pop = state->var_pop.count > 1 ? (state->var_pop.dsquared / state->var_pop.count) : 0;
			if (!Value::DoubleIsValid(var_pop)) {
				throw OutOfRangeException("VARPOP is out of range!");
			}
			target[idx] = cov / var_pop;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};
} // namespace duckdb
