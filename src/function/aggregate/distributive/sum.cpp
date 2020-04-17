#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"

using namespace std;

namespace duckdb {

struct sum_state_t {
	double value;
	bool isset;
};

struct SumOperation {
	template <class STATE> static void Initialize(STATE *state) {
		state->value = 0;
		state->isset = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		state->isset = true;
		state->value += input[idx];
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		state->isset = true;
		state->value += (double)input[0] * (double)count;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->isset) {
			nullmask[idx] = true;
		} else {
			if (!Value::DoubleIsValid(state->value)) {
				throw OutOfRangeException("SUM is out of range!");
			}
			target[idx] = state->value;
		}
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target->isset) {
			// target is NULL, use source value directly
			*target = source;
		} else {
			// else perform the operation
			target->value += source.value;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

void SumFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet sum("sum");
	// integer sums to bigint
	sum.AddFunction(AggregateFunction::UnaryAggregate<sum_state_t, int32_t, double, SumOperation>(SQLType::INTEGER,
	                                                                                              SQLType::DOUBLE));
	sum.AddFunction(AggregateFunction::UnaryAggregate<sum_state_t, int64_t, double, SumOperation>(SQLType::BIGINT,
	                                                                                              SQLType::DOUBLE));
	// float sums to float
	sum.AddFunction(
	    AggregateFunction::UnaryAggregate<sum_state_t, double, double, SumOperation>(SQLType::DOUBLE, SQLType::DOUBLE));

	set.AddFunction(sum);
}

} // namespace duckdb
