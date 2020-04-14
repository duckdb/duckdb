#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"

using namespace std;

namespace duckdb {

struct SumOperation : public StandardDistributiveFunction {
	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE *state, INPUT_TYPE input) {
		*state = input;
	}

	template <class INPUT_TYPE, class STATE> static void Execute(STATE *state, INPUT_TYPE input) {
		*state += input;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		assert(!nullmask[0]);
		if (IsNullValue<INPUT_TYPE>(*state)) {
			*state = 0;
		}
		*state += input[0] * count;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		nullmask[idx] = IsNullValue<T>(*state);
		target[idx] = *state;
	}
};

template <>
void SumOperation::Finalize(Vector &result, double *state, double *target, nullmask_t &nullmask, idx_t idx) {
	if (!Value::DoubleIsValid(*state)) {
		throw OutOfRangeException("SUM is out of range!");
	}
	nullmask[idx] = IsNullValue<double>(*state);
	target[idx] = *state;
}

void SumFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet sum("sum");
	// integer sums to bigint
	sum.AddFunction(
	    AggregateFunction::UnaryAggregate<int64_t, int64_t, int64_t, SumOperation>(SQLType::BIGINT, SQLType::BIGINT));
	// float sums to float
	sum.AddFunction(
	    AggregateFunction::UnaryAggregate<double, double, double, SumOperation>(SQLType::DOUBLE, SQLType::DOUBLE));

	set.AddFunction(sum);
}

} // namespace duckdb
