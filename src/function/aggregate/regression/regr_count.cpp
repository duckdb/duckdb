#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/regression_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb{
struct RegrCountFunction {
		template <class STATE>
	static void Initialize(STATE *state) {
		*state = 0;
	}

	template <class STATE, class OP>
	static void Combine(STATE source, STATE *target) {
		*target +=source;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
			target[idx] = *state;
	}
	static bool IgnoreNull() {
		return true;
	}
		template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask,
	                      nullmask_t &bnullmask, idx_t xidx, idx_t yidx)  {
		    *state+=1;
	}
};

void RegrCountFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet corr("regr_count");
	corr.AddFunction(AggregateFunction::BinaryAggregate<size_t , double, double, uint32_t , RegrCountFunction>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::UINTEGER));
	set.AddFunction(corr);
}

}
