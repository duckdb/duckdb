#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/function_set.hpp"

using namespace duckdb;
using namespace std;

template <class T> struct avg_state_t {
	uint64_t count;
	T sum;
};

struct AverageFunction {
	template <class STATE> static void Initialize(STATE *state) {
		state->count = 0;
		state->sum = 0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		state->sum += input[idx];
		state->count++;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		state->count += count;
		state->sum += input[0] * count;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		target->count += source.count;
		target->sum += source.sum;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!Value::DoubleIsValid(state->sum)) {
			throw OutOfRangeException("AVG is out of range!");
		} else if (state->count == 0) {
			nullmask[idx] = true;
		} else {
			target[idx] = state->sum / state->count;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

void AvgFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet avg("avg");
	avg.AddFunction(AggregateFunction::UnaryAggregate<avg_state_t<double>, double, double, AverageFunction>(
	    SQLType::DOUBLE, SQLType::DOUBLE));
	set.AddFunction(avg);
}
