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
#include "function/scalar_function.hpp"

namespace duckdb {

void round_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result);
bool round_matches_arguments(vector<SQLType> &arguments);
SQLType round_get_return_type(vector<SQLType> &arguments);

class RoundFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("round", round_matches_arguments, round_get_return_type, round_function);
	}
};

} // namespace duckdb
