#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/types/null_value.hpp"

using namespace std;

namespace duckdb {

struct MinMaxBase : public StandardDistributiveFunction {
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		assert(!nullmask[0]);
		if (IsNullValue<INPUT_TYPE>(*state)) {
			*state = input[0];
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input[0]);
		}
	}
};

struct MinOperation : public MinMaxBase {
	template <class INPUT_TYPE, class STATE> static void Execute(STATE *state, INPUT_TYPE input) {
		if (LessThan::Operation<INPUT_TYPE>(input, *state)) {
			*state = input;
		}
	}
};

struct MaxOperation : public MinMaxBase {
	template <class INPUT_TYPE, class STATE> static void Execute(STATE *state, INPUT_TYPE input) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, *state)) {
			*state = input;
		}
	}
};

void MinFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet min("min");
	for (auto type : SQLType::ALL_TYPES) {
		min.AddFunction(AggregateFunction::GetUnaryAggregate<MinOperation>(type));
	}
	set.AddFunction(min);
}

void MaxFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet max("max");
	for (auto type : SQLType::ALL_TYPES) {
		max.AddFunction(AggregateFunction::GetUnaryAggregate<MaxOperation>(type));
	}
	set.AddFunction(max);
}

} // namespace duckdb
