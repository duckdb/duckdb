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
	static void Combine(STATE source, STATE *target) {
		RegrCountFunction::Combine<size_t, OP>(source.count, &target->count);
		STDDevBaseOperation::Combine<StddevState, OP>(source.var_pop, &target->var_pop);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *fd, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (state->var_pop.count == 0) {
			nullmask[idx] = true;
			return;
		}
		auto var_pop = state->var_pop.count > 1 ? (state->var_pop.dsquared / state->var_pop.count) : 0;
		if (!Value::DoubleIsValid(var_pop)) {
			throw OutOfRangeException("VARPOP is out of range!");
		}
		RegrCountFunction::Finalize<T, size_t>(result, fd, &state->count, target, nullmask, idx);
		target[idx] *= var_pop;
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct RegrSXXOperation : RegrBaseOperation {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask,
	                      nullmask_t &bnullmask, idx_t xidx, idx_t yidx) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(&state->count, bind_data, y_data, x_data, bnullmask,
		                                                         anullmask, yidx, xidx);
		STDDevBaseOperation::Operation<A_TYPE, StddevState, OP>(&state->var_pop, bind_data, y_data, bnullmask, yidx);
	}
};

struct RegrSYYOperation : RegrBaseOperation {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask,
	                      nullmask_t &bnullmask, idx_t xidx, idx_t yidx) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(&state->count, bind_data, y_data, x_data, bnullmask,
		                                                         anullmask, yidx, xidx);
		STDDevBaseOperation::Operation<A_TYPE, StddevState, OP>(&state->var_pop, bind_data, x_data, bnullmask, xidx);
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
