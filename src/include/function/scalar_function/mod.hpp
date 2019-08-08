//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/mod.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "execution/expression_executor.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

void mod_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);
bool mod_matches_arguments(vector<SQLType> &arguments);
SQLType mod_get_return_type(vector<SQLType> &arguments);

class ModFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("mod", mod_matches_arguments, mod_get_return_type, mod_function);
	}
};

} // namespace duckdb
