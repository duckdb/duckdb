//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/regexp_matches.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

void regexp_matches_function(ExpressionExecutor &exec, Vector inputs[], size_t input_count,
                             BoundFunctionExpression &expr, Vector &result);
bool regexp_matches_matches_arguments(vector<SQLType> &arguments);
SQLType regexp_matches_get_return_type(vector<SQLType> &arguments);
unique_ptr<FunctionData> regexp_matches_get_bind_function(BoundFunctionExpression &expr, ClientContext &context);

class RegexpMatchesFunction {
public:
	static const char *GetName() {
		return "regexp_matches";
	}

	static scalar_function_t GetFunction() {
		return regexp_matches_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return regexp_matches_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return regexp_matches_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return regexp_matches_get_bind_function;
	}

	static bool HasSideEffects() {
		return false;
	}
};

} // namespace function
} // namespace duckdb
