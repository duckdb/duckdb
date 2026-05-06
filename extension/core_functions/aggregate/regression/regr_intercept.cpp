//! AVG(y)-REGR_SLOPE(y,x)*AVG(x)

#include "core_functions/aggregate/regression_functions.hpp"
#include "core_functions/aggregate/regression/regr_slope.hpp"
#include "core_functions/aggregate/algebraic_functions.hpp"

namespace duckdb {

namespace {
struct RegrInterceptState {
	uint64_t count;
	double sum_x;
	double sum_y;
	RegrSlopeState slope;
};

struct RegrInterceptOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.count = 0;
		state.sum_x = 0;
		state.sum_y = 0;
		RegrSlopeOperation::Initialize<RegrSlopeState>(state.slope);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		state.count++;
		state.sum_x += x;
		state.sum_y += y;
		RegrSlopeOperation::Operation<A_TYPE, B_TYPE, RegrSlopeState, OP>(state.slope, y, x, idata);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		target.count += source.count;
		target.sum_x += source.sum_x;
		target.sum_y += source.sum_y;
		RegrSlopeOperation::Combine<RegrSlopeState, OP>(source.slope, target.slope, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
			return;
		}
		RegrSlopeOperation::Finalize<T, RegrSlopeState>(state.slope, target, finalize_data);
		if (Value::IsNan(target)) {
			finalize_data.ReturnNull();
			return;
		}
		auto x_avg = state.sum_x / state.count;
		auto y_avg = state.sum_y / state.count;
		target = y_avg - target * x_avg;
	}

	static bool IgnoreNull() {
		return true;
	}
};

LogicalType GetRegrInterceptStateType(const BoundAggregateFunction &) {
	child_list_t<LogicalType> covpop_children;
	covpop_children.emplace_back("count", LogicalType::UBIGINT);
	covpop_children.emplace_back("meanx", LogicalType::DOUBLE);
	covpop_children.emplace_back("meany", LogicalType::DOUBLE);
	covpop_children.emplace_back("co_moment", LogicalType::DOUBLE);
	auto covpop_type = LogicalType::STRUCT(std::move(covpop_children));

	child_list_t<LogicalType> varpop_children;
	varpop_children.emplace_back("count", LogicalType::UBIGINT);
	varpop_children.emplace_back("mean", LogicalType::DOUBLE);
	varpop_children.emplace_back("dsquared", LogicalType::DOUBLE);
	auto varpop_type = LogicalType::STRUCT(std::move(varpop_children));

	child_list_t<LogicalType> state_children;
	state_children.emplace_back("count", LogicalType::UBIGINT);
	state_children.emplace_back("sum_x", LogicalType::DOUBLE);
	state_children.emplace_back("sum_y", LogicalType::DOUBLE);
	child_list_t<LogicalType> slope_children;
	slope_children.emplace_back("cov_pop", std::move(covpop_type));
	slope_children.emplace_back("var_pop", std::move(varpop_type));
	state_children.emplace_back("slope", LogicalType::STRUCT(std::move(slope_children)));
	return LogicalType::STRUCT(std::move(state_children));
}

} // namespace

AggregateFunction RegrInterceptFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrInterceptState, double, double, double, RegrInterceptOperation>(
	           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE)
	    .SetStructStateExport(GetRegrInterceptStateType);
}

} // namespace duckdb
