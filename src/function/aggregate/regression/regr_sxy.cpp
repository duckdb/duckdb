// Returns REGR_COUNT(expr1, expr2) * COVAR_POP(expr1, expr2) for non-null pairs.

#include "duckdb/function/aggregate/regression/regr_count.hpp"
#include "duckdb/function/aggregate/algebraic/covar.hpp"
#include "duckdb/function/aggregate/regression_functions.hpp"
namespace duckdb {
struct regr_sxy_state_t {
	size_t count;
	covar_state_t cov_pop;
};

struct RegrSXYOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		RegrCountFunction::Initialize<size_t>(&state->count);
		CovarOperation::Initialize<covar_state_t>(&state->cov_pop);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask,
	                      nullmask_t &bnullmask, idx_t xidx, idx_t yidx) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(&state->count, bind_data, y_data, x_data, bnullmask,
		                                                         anullmask, yidx, xidx);
		CovarOperation::Operation<A_TYPE, B_TYPE, covar_state_t, OP>(&state->cov_pop, bind_data, x_data, y_data,
		                                                             anullmask, bnullmask, xidx, yidx);
	}

	template <class STATE, class OP>
	static void Combine(STATE source, STATE *target) {
		CovarOperation::Combine<covar_state_t, OP>(source.cov_pop, &target->cov_pop);
		RegrCountFunction::Combine<size_t, OP>(source.count, &target->count);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *fd, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		CovarPopOperation::Finalize<T, covar_state_t>(result, fd, &state->cov_pop, target, nullmask, idx);
		auto cov_pop = target[idx];
		RegrCountFunction::Finalize<T, size_t>(result, fd, &state->count, target, nullmask, idx);
		target[idx] *= cov_pop;
	}

	static bool IgnoreNull() {
		return true;
	}
};

void RegrSXYFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("regr_sxy");
	fun.AddFunction(AggregateFunction::BinaryAggregate<regr_sxy_state_t, double, double, double, RegrSXYOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(fun);
}

} // namespace duckdb
