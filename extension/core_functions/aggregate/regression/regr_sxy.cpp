// REGR_SXY(y, x)
// Returns REGR_COUNT(expr1, expr2) * COVAR_POP(expr1, expr2) for non-null pairs.

#include "core_functions/aggregate/regression/regr_count.hpp"
#include "core_functions/aggregate/algebraic/covar.hpp"
#include "core_functions/aggregate/regression_functions.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

namespace {

struct RegrSXyState {
	size_t count;
	CovarState cov_pop;
};

struct RegrSXYOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		RegrCountFunction::Initialize<size_t>(state.count);
		CovarOperation::Initialize<CovarState>(state.cov_pop);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(state.count, y, x, idata);
		CovarOperation::Operation<A_TYPE, B_TYPE, CovarState, OP>(state.cov_pop, y, x, idata);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		CovarOperation::Combine<CovarState, OP>(source.cov_pop, target.cov_pop, aggr_input_data);
		RegrCountFunction::Combine<size_t, OP>(source.count, target.count, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		CovarPopOperation::Finalize<T, CovarState>(state.cov_pop, target, finalize_data);
		auto cov_pop = target;
		RegrCountFunction::Finalize<T, size_t>(state.count, target, finalize_data);
		target *= cov_pop;
	}

	static bool IgnoreNull() {
		return true;
	}
};

LogicalType GetRegrSXYStateType(const AggregateFunction &) {
	child_list_t<LogicalType> state_children;
	state_children.emplace_back("count", LogicalType::UBIGINT);
	child_list_t<LogicalType> cov_pop_children;
	cov_pop_children.emplace_back("count", LogicalType::UBIGINT);
	cov_pop_children.emplace_back("meanx", LogicalType::DOUBLE);
	cov_pop_children.emplace_back("meany", LogicalType::DOUBLE);
	cov_pop_children.emplace_back("co_moment", LogicalType::DOUBLE);
	state_children.emplace_back("cov_pop", LogicalType::STRUCT(std::move(cov_pop_children)));
	return LogicalType::STRUCT(std::move(state_children));
}

} // namespace

AggregateFunction RegrSXYFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSXyState, double, double, double, RegrSXYOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetRegrSXYStateType);
}

} // namespace duckdb
