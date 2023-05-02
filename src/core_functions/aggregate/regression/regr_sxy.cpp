// Returns REGR_COUNT(expr1, expr2) * COVAR_POP(expr1, expr2) for non-null pairs.

#include "duckdb/core_functions/aggregate/regression/regr_count.hpp"
#include "duckdb/core_functions/aggregate/algebraic/covar.hpp"
#include "duckdb/core_functions/aggregate/regression_functions.hpp"
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
	static void Operation(STATE *state, AggregateInputData &aggr_input_data, A_TYPE *x_data, B_TYPE *y_data,
	                      ValidityMask &amask, ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(&state->count, aggr_input_data, y_data, x_data, bmask,
		                                                         amask, yidx, xidx);
		CovarOperation::Operation<A_TYPE, B_TYPE, CovarState, OP>(&state->cov_pop, aggr_input_data, x_data, y_data,
		                                                          amask, bmask, xidx, yidx);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &aggr_input_data) {
		CovarOperation::Combine<CovarState, OP>(source.cov_pop, &target->cov_pop, aggr_input_data);
		RegrCountFunction::Combine<size_t, OP>(source.count, &target->count, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &aggr_input_data, STATE *state, T *target,
	                     ValidityMask &mask, idx_t idx) {
		CovarPopOperation::Finalize<T, CovarState>(result, aggr_input_data, &state->cov_pop, target, mask, idx);
		auto cov_pop = target[idx];
		RegrCountFunction::Finalize<T, size_t>(result, aggr_input_data, &state->count, target, mask, idx);
		target[idx] *= cov_pop;
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction RegrSXYFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSXyState, double, double, double, RegrSXYOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb
