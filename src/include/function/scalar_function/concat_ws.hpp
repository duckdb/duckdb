//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/concat_ws.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "execution/expression_executor.hpp"
#include "function/function.hpp"

namespace duckdb {

void concat_ws_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                        Vector &result);
bool concat_ws_match_input_types(vector<SQLType> &arguments);
bool concat_ws_matches_arguments(vector<SQLType> &arguments);
SQLType concat_ws_get_return_type(vector<SQLType> &arguments);

class ConcatWSFunction {
public:
	static const char *GetName() {
		return "concat_ws";
	}

	static scalar_function_t GetFunction() {
		return concat_ws_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return concat_ws_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return concat_ws_get_return_type;
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
