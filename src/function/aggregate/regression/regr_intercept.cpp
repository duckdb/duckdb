//! AVG(y)-REGR_SLOPE(y,x)*AVG(x)

#include "duckdb/function/aggregate/regression_functions.hpp"
#include "duckdb/function/aggregate/regression/regr_slope.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct RegrInterceptState {
	size_t count;
	double sum_x;
	double sum_y;
	RegrSlopeState slope;
};

struct RegrInterceptOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->count = 0;
		state->sum_x = 0;
		state->sum_y = 0;
		RegrSlopeOperation::Initialize<RegrSlopeState>(&state->slope);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, ValidityMask &amask,
	                      ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		state->count++;
		state->sum_x += y_data[yidx];
		state->sum_y += x_data[xidx];
		RegrSlopeOperation::Operation<A_TYPE, B_TYPE, RegrSlopeState, OP>(&state->slope, bind_data, x_data, y_data,
		                                                                  amask, bmask, xidx, yidx);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, FunctionData *bind_data) {
		target->count += source.count;
		target->sum_x += source.sum_x;
		target->sum_y += source.sum_y;
		RegrSlopeOperation::Combine<RegrSlopeState, OP>(source.slope, &target->slope, bind_data);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *fd, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
			return;
		}
		RegrSlopeOperation::Finalize<T, RegrSlopeState>(result, fd, &state->slope, target, mask, idx);
		auto x_avg = state->sum_x / state->count;
		auto y_avg = state->sum_y / state->count;
		target[idx] = y_avg - target[idx] * x_avg;
	}

	static bool IgnoreNull() {
		return true;
	}
};

void RegrInterceptFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("regr_intercept");
	fun.AddFunction(
	    AggregateFunction::BinaryAggregate<RegrInterceptState, double, double, double, RegrInterceptOperation>(
	        LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(fun);
}

} // namespace duckdb
