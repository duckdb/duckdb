//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/caseconvert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {

void caseconvert_upper_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                BoundFunctionExpression &expr, Vector &result);
void caseconvert_lower_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                BoundFunctionExpression &expr, Vector &result);

bool caseconvert_matches_arguments(vector<SQLType> &arguments);
SQLType caseconvert_get_return_type(vector<SQLType> &arguments);

class UpperFunction {
public:
	static const char *GetName() {
		return "upper";
	}

	static scalar_function_t GetFunction() {
		return caseconvert_upper_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return caseconvert_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return caseconvert_get_return_type;
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

class LowerFunction {
public:
	static const char *GetName() {
		return "lower";
	}

	static scalar_function_t GetFunction() {
		return caseconvert_lower_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return caseconvert_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return caseconvert_get_return_type;
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

} // namespace duckdb
