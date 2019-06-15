//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/round.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "execution/expression_executor.hpp"
#include "function/function.hpp"

namespace duckdb {

void round_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result);
bool round_matches_arguments(vector<SQLType> &arguments);
SQLType round_get_return_type(vector<SQLType> &arguments);

class RoundFunction {
public:
	static const char *GetName() {
		return "round";
	}

	static scalar_function_t GetFunction() {
		return round_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return round_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return round_get_return_type;
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
