//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/nextval.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "execution/expression_executor.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

void nextval_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                      Vector &result);
bool nextval_matches_arguments(vector<SQLType> &arguments);
SQLType nextval_get_return_type(vector<SQLType> &arguments);
unique_ptr<FunctionData> nextval_bind(BoundFunctionExpression &expr, ClientContext &context);
void nextval_dependency(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies);

class NextvalFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("nextval", nextval_matches_arguments, nextval_get_return_type, nextval_function, true, nextval_bind, nextval_dependency);
	}
};

} // namespace duckdb
