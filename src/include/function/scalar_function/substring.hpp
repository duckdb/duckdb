//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/substring.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {

void substring_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                        Vector &result);
bool substring_matches_arguments(vector<SQLType> &arguments);
SQLType substring_get_return_type(vector<SQLType> &arguments);

class SubstringFunction {
public:
	static const char *GetName() {
		return "substring";
	}

	static scalar_function_t GetFunction() {
		return substring_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return substring_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return substring_get_return_type;
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
