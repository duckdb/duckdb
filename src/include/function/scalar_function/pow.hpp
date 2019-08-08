//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/pow.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "execution/expression_executor.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

void pow_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);
bool pow_matches_arguments(vector<SQLType> &arguments);
SQLType pow_get_return_type(vector<SQLType> &arguments);

class PowFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("pow", pow_matches_arguments, pow_get_return_type, pow_function);
	}
};
class PowerFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("power", pow_matches_arguments, pow_get_return_type, pow_function);
	}
};

} // namespace duckdb
