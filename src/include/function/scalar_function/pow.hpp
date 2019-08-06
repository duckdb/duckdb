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
#include "function/function.hpp"

namespace duckdb {

void pow_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);
bool pow_matches_arguments(vector<SQLType> &arguments);
SQLType pow_get_return_type(vector<SQLType> &arguments);

class PowFunction {
public:
	static const char *GetName() {
		return "pow";
	}

	static scalar_function_t GetFunction() {
		return pow_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return pow_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return pow_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};
class PowerFunction : public PowFunction {
public:
	static const char *GetName() {
		return "power";
	}
};

} // namespace duckdb
