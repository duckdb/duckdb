//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/concat.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "execution/expression_executor.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

void concat_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                     Vector &result);
bool concat_matches_arguments(vector<SQLType> &arguments);
SQLType concat_get_return_type(vector<SQLType> &arguments);

class ConcatFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("concat", concat_matches_arguments, concat_get_return_type, concat_function);
	}
};

} // namespace duckdb
