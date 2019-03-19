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
namespace function {

void abs_function(Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result);
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
};

} // namespace function
} // namespace duckdb
