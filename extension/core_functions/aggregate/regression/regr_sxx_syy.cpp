// REGR_SXX(y, x)
// Returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs.
// REGR_SYY(y, x)
// Returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs.

#include "core_functions/aggregate/regression/regr_count.hpp"
#include "duckdb/function/function_set.hpp"
#include "core_functions/aggregate/regression_functions.hpp"

namespace duckdb {

struct RegrSState {
	size_t count;
	StddevState var_pop;
};

struct RegrBaseOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		RegrCountFunction::Initialize<size_t>(state.count);
		STDDevBaseOperation::Initialize<StddevState>(state.var_pop);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		RegrCountFunction::Combine<size_t, OP>(source.count, target.count, aggr_input_data);
		STDDevBaseOperation::Combine<StddevState, OP>(source.var_pop, target.var_pop, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.var_pop.count == 0) {
			finalize_data.ReturnNull();
			return;
		}
		auto var_pop = state.var_pop.count > 1 ? (state.var_pop.dsquared / state.var_pop.count) : 0;
		if (!Value::DoubleIsFinite(var_pop)) {
			throw OutOfRangeException("VARPOP is out of range!");
		}
		RegrCountFunction::Finalize<T, size_t>(state.count, target, finalize_data);
		target *= var_pop;
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct RegrSXXOperation : RegrBaseOperation {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(state.count, y, x, idata);
		STDDevBaseOperation::Execute<A_TYPE, StddevState>(state.var_pop, x);
	}
};

struct RegrSYYOperation : RegrBaseOperation {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(state.count, y, x, idata);
		STDDevBaseOperation::Execute<A_TYPE, StddevState>(state.var_pop, y);
	}
};

AggregateFunction RegrSXXFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSState, double, double, double, RegrSXXOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

AggregateFunction RegrSYYFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSState, double, double, double, RegrSYYOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb
