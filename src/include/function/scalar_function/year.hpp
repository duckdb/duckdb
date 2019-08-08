//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/year.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

void year_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result);
bool year_matches_arguments(vector<SQLType> &arguments);
SQLType year_get_return_type(vector<SQLType> &arguments);

class YearFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("year", year_matches_arguments, year_get_return_type, year_function);
	}
};

} // namespace duckdb
