//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/mod.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "execution/expression_executor.hpp"
#include "function/function.hpp"

namespace duckdb {

void mod_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);
bool mod_matches_arguments(vector<SQLType> &arguments);
SQLType mod_get_return_type(vector<SQLType> &arguments);

class ModFunction {
public:
	static const char *GetName() {
		return "mod";
	}

	static scalar_function_t GetFunction() {
		return mod_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return mod_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return mod_get_return_type;
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
