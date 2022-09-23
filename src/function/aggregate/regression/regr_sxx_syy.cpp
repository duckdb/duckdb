// regr_sxx
// Returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs.
// regrsyy
// Returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs.

#include "duckdb/function/aggregate/regression/regr_count.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/aggregate/regression_functions.hpp"

namespace duckdb {

struct RegrSState {
	size_t count;
	StddevState var_pop;
};

struct RegrBaseOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		RegrCountFunction::Initialize<size_t>(&state->count);
		STDDevBaseOperation::Initialize<StddevState>(&state->var_pop);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &aggr_input_data) {
		RegrCountFunction::Combine<size_t, OP>(source.count, &target->count, aggr_input_data);
		STDDevBaseOperation::Combine<StddevState, OP>(source.var_pop, &target->var_pop, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &aggr_input_data, STATE *state, T *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->var_pop.count == 0) {
			mask.SetInvalid(idx);
			return;
		}
		auto var_pop = state->var_pop.count > 1 ? (state->var_pop.dsquared / state->var_pop.count) : 0;
		if (!Value::DoubleIsFinite(var_pop)) {
			throw OutOfRangeException("VARPOP is out of range!");
		}
		RegrCountFunction::Finalize<T, size_t>(result, aggr_input_data, &state->count, target, mask, idx);
		target[idx] *= var_pop;
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct RegrSXXOperation : RegrBaseOperation {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &aggr_input_data, A_TYPE *x_data, B_TYPE *y_data,
	                      ValidityMask &amask, ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(&state->count, aggr_input_data, y_data, x_data, bmask,
		                                                         amask, yidx, xidx);
		STDDevBaseOperation::Operation<A_TYPE, StddevState, OP>(&state->var_pop, aggr_input_data, y_data, bmask, yidx);
	}
};

struct RegrSYYOperation : RegrBaseOperation {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &aggr_input_data, A_TYPE *x_data, B_TYPE *y_data,
	                      ValidityMask &amask, ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(&state->count, aggr_input_data, y_data, x_data, bmask,
		                                                         amask, yidx, xidx);
		STDDevBaseOperation::Operation<A_TYPE, StddevState, OP>(&state->var_pop, aggr_input_data, x_data, bmask, xidx);
	}
};

void RegrSXXFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("regr_sxx");
	fun.AddFunction(AggregateFunction::BinaryAggregate<RegrSState, double, double, double, RegrSXXOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(fun);
}

void RegrSYYFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("regr_syy");
	fun.AddFunction(AggregateFunction::BinaryAggregate<RegrSState, double, double, double, RegrSYYOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(fun);
}

} // namespace duckdb
