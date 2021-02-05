#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

struct BoolAndFunFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		*state = true;
	}

	template <class STATE, class OP>
	static void Combine(STATE source, STATE *target) {
		*target = source && *target;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		target[idx] = *state;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		*state = input[idx] && *state;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, nullmask, 0);
		}
	}
	static bool IgnoreNull() {
		return true;
	}
};

struct BoolOrFunFunction  {
	template <class STATE>
	static void Initialize(STATE *state) {
		*state = false;
	}

	template <class STATE, class OP>
	static void Combine(STATE source, STATE *target) {
		*target = source || *target;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		target[idx] = *state;
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		*state = input[idx] || *state;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, nullmask, 0);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction BoolOrFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<bool, bool, bool, BoolOrFunFunction>(LogicalType(LogicalTypeId::BOOLEAN),
	                                                                              LogicalType::BOOLEAN);
}

AggregateFunction BoolAndFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<bool, bool, bool, BoolAndFunFunction>(LogicalType(LogicalTypeId::BOOLEAN),
	                                                                               LogicalType::BOOLEAN);
}

void BoolOrFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunction bool_or_function = BoolOrFun::GetFunction();
	AggregateFunctionSet bool_or("bool_or");
	bool_or.AddFunction(bool_or_function);
	set.AddFunction(bool_or);
}

void BoolAndFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunction bool_and_function = BoolAndFun::GetFunction();
	AggregateFunctionSet bool_and("bool_and");
	bool_and.AddFunction(bool_and_function);
	set.AddFunction(bool_and);
}


} // namespace duckdb
