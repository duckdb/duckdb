// Returns REGR_COUNT(expr1, expr2) * COVAR_POP(expr1, expr2) for non-null pairs.

#include "duckdb/function/aggregate/regression/regr_count.hpp"
#include "duckdb/function/aggregate/algebraic/covar.hpp"
#include "duckdb/function/aggregate/regression_functions.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct RegrSXyState {
	size_t count;
	CovarState cov_pop;
};

struct RegrSXYOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		RegrCountFunction::Initialize<size_t>(&state->count);
		CovarOperation::Initialize<CovarState>(&state->cov_pop);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, ValidityMask &amask,
	                      ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(&state->count, bind_data, y_data, x_data, bmask, amask,
		                                                         yidx, xidx);
		CovarOperation::Operation<A_TYPE, B_TYPE, CovarState, OP>(&state->cov_pop, bind_data, x_data, y_data, amask,
		                                                          bmask, xidx, yidx);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, FunctionData *bind_data) {
		CovarOperation::Combine<CovarState, OP>(source.cov_pop, &target->cov_pop, bind_data);
		RegrCountFunction::Combine<size_t, OP>(source.count, &target->count, bind_data);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *fd, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		CovarPopOperation::Finalize<T, CovarState>(result, fd, &state->cov_pop, target, mask, idx);
		auto cov_pop = target[idx];
		RegrCountFunction::Finalize<T, size_t>(result, fd, &state->count, target, mask, idx);
		target[idx] *= cov_pop;
	}

	static bool IgnoreNull() {
		return true;
	}
};

void RegrSXYFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("regr_sxy");
	fun.AddFunction(AggregateFunction::BinaryAggregate<RegrSXyState, double, double, double, RegrSXYOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(fun);
}

} // namespace duckdb
