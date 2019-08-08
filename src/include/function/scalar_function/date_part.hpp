//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/date_part.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

void date_part_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                        Vector &result);
bool date_part_matches_arguments(vector<SQLType> &arguments);
SQLType date_part_get_return_type(vector<SQLType> &arguments);

class DatePartFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("date_part", date_part_matches_arguments, date_part_get_return_type, date_part_function);
	}
};

} // namespace duckdb
