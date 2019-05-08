//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/abs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {

void abs_function(ExpressionExecutor &exec, Vector inputs[], uint64_t input_count, BoundFunctionExpression &expr,
                  Vector &result);
bool abs_matches_arguments(vector<SQLType> &arguments);
SQLType abs_get_return_type(vector<SQLType> &arguments);

class AbsFunction {
public:
	static const char *GetName() {
		return "abs";
	}

	static scalar_function_t GetFunction() {
		return abs_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return abs_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return abs_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

} // namespace duckdb
