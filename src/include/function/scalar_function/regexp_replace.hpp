//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/regexp_replace.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {

void regexp_replace_function(ExpressionExecutor &exec, Vector inputs[], uint64_t input_count,
                             BoundFunctionExpression &expr, Vector &result);
bool regexp_replace_matches_arguments(vector<SQLType> &arguments);
SQLType regexp_replace_get_return_type(vector<SQLType> &arguments);

class RegexpReplaceFunction {
public:
	static const char *GetName() {
		return "regexp_replace";
	}

	static scalar_function_t GetFunction() {
		return regexp_replace_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return regexp_replace_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return regexp_replace_get_return_type;
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
