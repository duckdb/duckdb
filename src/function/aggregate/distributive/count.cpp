#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

struct BaseCountFunction {
	template <class STATE> static void Initialize(STATE *state) {
		*state = 0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		*state += 1;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		*state += count;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		*target += source;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		target[idx] = *state;
	}
};

struct CountStarFunction : public BaseCountFunction {
	static bool IgnoreNull() {
		return false;
	}
};

struct CountFunction : public BaseCountFunction {
	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction CountFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<int64_t, int64_t, int64_t, CountFunction>(SQLType(SQLTypeId::ANY),
	                                                                                   SQLType::BIGINT);
}

AggregateFunction CountStarFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<int64_t, int64_t, int64_t, CountStarFunction>(SQLType(SQLTypeId::ANY),
	                                                                                       SQLType::BIGINT);
}

void CountFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunction count_function = CountFun::GetFunction();
	AggregateFunctionSet count("count");
	count.AddFunction(count_function);
	// the count function can also be called without arguments
	count_function.arguments.clear();
	count.AddFunction(count_function);
	set.AddFunction(count);
}

void CountStarFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet count("count_star");
	count.AddFunction(CountStarFun::GetFunction());
	set.AddFunction(count);
}

} // namespace duckdb
