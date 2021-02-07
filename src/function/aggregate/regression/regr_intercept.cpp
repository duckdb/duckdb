//! AVG(y)-REGR_SLOPE(y,x)*AVG(x)

#include "duckdb/function/aggregate/regression_functions.hpp"
#include "duckdb/function/aggregate/regression/regr_slope.hpp"

namespace duckdb {
struct regr_intercept_state_t {
	size_t count;
	double sum_x;
	double sum_y;
	regr_slope_state_t slope;
};

struct RegrInterceptOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->count = 0;
		state->sum_x = 0;
		state->sum_y = 0;
		RegrSlopeOperation::Initialize<regr_slope_state_t>(&state->slope);
	}

			template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask,
	                      nullmask_t &bnullmask, idx_t xidx, idx_t yidx) {
        state->count++;
		state->sum_x += y_data[yidx];
		state->sum_y += x_data[xidx];
		RegrSlopeOperation::Operation<A_TYPE,B_TYPE,regr_slope_state_t,OP>(&state->slope,bind_data,x_data,y_data,anullmask,bnullmask,xidx,yidx);
	}

	template <class STATE, class OP>
	static void Combine(STATE source, STATE *target) {
		target->count += source.count;
		target->sum_x += source.sum_x;
		target->sum_y += source.sum_y;
		RegrSlopeOperation::Combine<regr_slope_state_t, OP>(source.slope, &target->slope);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData * fd, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
        RegrSlopeOperation::Finalize<T,regr_slope_state_t>(result,fd,&state->slope,target,nullmask,idx);
        auto x_avg = state->sum_x/state->count;
		auto y_avg = state->sum_y/state->count;
		target[idx] =y_avg - target[idx]*x_avg;
	}

	static bool IgnoreNull() {
		return true;
	}
};


void RegrInterceptFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("regr_intercept");
	fun.AddFunction(AggregateFunction::BinaryAggregate<regr_intercept_state_t, double, double, double, RegrInterceptOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(fun);
}

} // namespace duckdb
