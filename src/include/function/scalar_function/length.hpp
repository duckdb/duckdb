//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/length.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

void length_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                     Vector &result);
bool length_matches_arguments(vector<SQLType> &arguments);
SQLType length_get_return_type(vector<SQLType> &arguments);

class LengthFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("length", length_matches_arguments, length_get_return_type, length_function);
	}
};

} // namespace duckdb
