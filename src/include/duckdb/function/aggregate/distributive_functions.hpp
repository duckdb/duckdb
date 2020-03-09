//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/distributive_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/types/null_value.hpp"

namespace duckdb {

struct StandardDistributiveFunction {
	template <class STATE> static void Initialize(STATE *state) {
		*state = NullValue<STATE>();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		if (IsNullValue<INPUT_TYPE>(*state)) {
			*state = input[idx];
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input[idx]);
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		nullmask[idx] = IsNullValue<T>(*state);
		target[idx] = *state;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (IsNullValue<STATE>(source)) {
			// source is NULL, nothing to do
			return;
		}
		if (IsNullValue<STATE>(*target)) {
			// target is NULL, use source value directly
			*target = source;
		} else {
			// else perform the operation
			OP::template Execute<STATE, STATE>(target, source);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct CountStarFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct CountFun {
	static AggregateFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct FirstFun {
	static AggregateFunction GetFunction(SQLType type);

	static void RegisterFunction(BuiltinFunctions &set);
};

struct MaxFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MinFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SumFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StringAggFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
