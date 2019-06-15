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
#include "function/function.hpp"

namespace duckdb {

void nextval_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                      Vector &result);
bool nextval_matches_arguments(vector<SQLType> &arguments);
SQLType nextval_get_return_type(vector<SQLType> &arguments);
unique_ptr<FunctionData> nextval_bind(BoundFunctionExpression &expr, ClientContext &context);
void nextval_dependency(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies);

class NextvalFunction {
public:
	static const char *GetName() {
		return "nextval";
	}

	static scalar_function_t GetFunction() {
		return nextval_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return nextval_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return nextval_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nextval_bind;
	}

	static dependency_function_t GetDependencyFunction() {
		return nextval_dependency;
	}

	static bool HasSideEffects() {
		return true;
	}
};

} // namespace duckdb
