// REGR_R2(y, x)
// Returns the coefficient of determination for non-null pairs in a group.
// It is computed for non-null pairs using the following formula:
// null                 if var_pop(x) = 0, else
// 1                    if var_pop(y) = 0 and var_pop(x) <> 0, else
// power(corr(y,x), 2)

#include "duckdb/core_functions/aggregate/algebraic/corr.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/core_functions/aggregate/regression_functions.hpp"

namespace duckdb {
struct RegrR2State {
	CorrState corr;
	StddevState var_pop_x;
	StddevState var_pop_y;
};

struct RegrR2Operation {
	template <class STATE>
	static void Initialize(STATE &state) {
		CorrOperation::Initialize<CorrState>(state.corr);
		STDDevBaseOperation::Initialize<StddevState>(state.var_pop_x);
		STDDevBaseOperation::Initialize<StddevState>(state.var_pop_y);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		CorrOperation::Operation<A_TYPE, B_TYPE, CorrState, OP>(state.corr, y, x, idata);
		STDDevBaseOperation::Execute<A_TYPE, StddevState>(state.var_pop_x, x);
		STDDevBaseOperation::Execute<A_TYPE, StddevState>(state.var_pop_y, y);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		CorrOperation::Combine<CorrState, OP>(source.corr, target.corr, aggr_input_data);
		STDDevBaseOperation::Combine<StddevState, OP>(source.var_pop_x, target.var_pop_x, aggr_input_data);
		STDDevBaseOperation::Combine<StddevState, OP>(source.var_pop_y, target.var_pop_y, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		auto var_pop_x = state.var_pop_x.count > 1 ? (state.var_pop_x.dsquared / state.var_pop_x.count) : 0;
		if (!Value::DoubleIsFinite(var_pop_x)) {
			throw OutOfRangeException("VARPOP(X) is out of range!");
		}
		if (var_pop_x == 0) {
			finalize_data.ReturnNull();
			return;
		}
		auto var_pop_y = state.var_pop_y.count > 1 ? (state.var_pop_y.dsquared / state.var_pop_y.count) : 0;
		if (!Value::DoubleIsFinite(var_pop_y)) {
			throw OutOfRangeException("VARPOP(Y) is out of range!");
		}
		if (var_pop_y == 0) {
			target = 1;
			return;
		}
		CorrOperation::Finalize<T, CorrState>(state.corr, target, finalize_data);
		target = pow(target, 2);
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction RegrR2Fun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrR2State, double, double, double, RegrR2Operation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}
} // namespace duckdb
