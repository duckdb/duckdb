//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/substring.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

void substring_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                        Vector &result);
bool substring_matches_arguments(vector<SQLType> &arguments);
SQLType substring_get_return_type(vector<SQLType> &arguments);

class SubstringFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("substring", substring_matches_arguments, substring_get_return_type, substring_function);
	}
};

} // namespace duckdb
