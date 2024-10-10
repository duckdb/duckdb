//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/algebraic/corr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/core_functions/aggregate/algebraic/covar.hpp"
#include "duckdb/core_functions/aggregate/algebraic/stddev.hpp"

namespace duckdb {

struct CorrState {
	CovarState cov_pop;
	StddevState dev_pop_x;
	StddevState dev_pop_y;
};

// Returns the correlation coefficient for non-null pairs in a group.
// CORR(y, x) = COVAR_POP(y, x) / (STDDEV_POP(x) * STDDEV_POP(y))
struct CorrOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		CovarOperation::Initialize<CovarState>(state.cov_pop);
		STDDevBaseOperation::Initialize<StddevState>(state.dev_pop_x);
		STDDevBaseOperation::Initialize<StddevState>(state.dev_pop_y);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		CovarOperation::Operation<A_TYPE, B_TYPE, CovarState, OP>(state.cov_pop, y, x, idata);
		STDDevBaseOperation::Execute<A_TYPE, StddevState>(state.dev_pop_x, x);
		STDDevBaseOperation::Execute<B_TYPE, StddevState>(state.dev_pop_y, y);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		CovarOperation::Combine<CovarState, OP>(source.cov_pop, target.cov_pop, aggr_input_data);
		STDDevBaseOperation::Combine<StddevState, OP>(source.dev_pop_x, target.dev_pop_x, aggr_input_data);
		STDDevBaseOperation::Combine<StddevState, OP>(source.dev_pop_y, target.dev_pop_y, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.cov_pop.count == 0 || state.dev_pop_x.count == 0 || state.dev_pop_y.count == 0) {
			finalize_data.ReturnNull();
		} else {
			auto cov = state.cov_pop.co_moment / state.cov_pop.count;
			auto std_x = state.dev_pop_x.count > 1 ? sqrt(state.dev_pop_x.dsquared / state.dev_pop_x.count) : 0;
			if (!Value::DoubleIsFinite(std_x)) {
				throw OutOfRangeException("STDDEV_POP for X is out of range!");
			}
			auto std_y = state.dev_pop_y.count > 1 ? sqrt(state.dev_pop_y.dsquared / state.dev_pop_y.count) : 0;
			if (!Value::DoubleIsFinite(std_y)) {
				throw OutOfRangeException("STDDEV_POP for Y is out of range!");
			}
			if (std_x * std_y == 0) {
				finalize_data.ReturnNull();
				return;
			}
			target = cov / (std_x * std_y);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

} // namespace duckdb
