#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/regression_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {
struct regr_state_t {
	double sum;
	size_t count;
};

struct RegrAvgFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->sum = 0;
		state->count = 0;
	}

	template <class STATE, class OP>
	static void Combine(STATE source, STATE *target) {
		target->sum += source.sum;
		target->count += source.count;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (state->count == 0) {
			nullmask.set(0, true);
		} else {
			target[idx] = state->sum / (double)state->count;
		}
	}
	static bool IgnoreNull() {
		return true;
	}
};
struct RegrAvgXFunction : RegrAvgFunction {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask,
	                      nullmask_t &bnullmask, idx_t xidx, idx_t yidx) {
		state->sum += y_data[yidx];
		state->count++;
	}
};

struct RegrAvgYFunction : RegrAvgFunction {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask,
	                      nullmask_t &bnullmask, idx_t xidx, idx_t yidx) {
		state->sum += x_data[xidx];
		state->count++;
	}
};

void RegrAvgxFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet corr("regr_avgx");
	corr.AddFunction(AggregateFunction::BinaryAggregate<regr_state_t, double, double, double, RegrAvgXFunction>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(corr);
}

void RegrAvgyFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet corr("regr_avgy");
	corr.AddFunction(AggregateFunction::BinaryAggregate<regr_state_t, double, double, double, RegrAvgYFunction>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(corr);
}

} // namespace duckdb
