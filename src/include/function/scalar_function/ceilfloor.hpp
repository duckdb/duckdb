//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/ceilfloor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {

void ceil_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

void floor_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                    Vector &result);

bool ceilfloor_matches_arguments(vector<SQLType> &arguments);
SQLType ceilfloor_get_return_type(vector<SQLType> &arguments);

class CeilFunction {
public:
	static const char *GetName() {
		return "ceil";
	}

	static scalar_function_t GetFunction() {
		return ceil_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return ceilfloor_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return ceilfloor_get_return_type;
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

class CeilingFunction {
public:
	static const char *GetName() {
		return "ceiling";
	}

	static scalar_function_t GetFunction() {
		return ceil_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return ceilfloor_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return ceilfloor_get_return_type;
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

class FloorFunction {
public:
	static const char *GetName() {
		return "floor";
	}

	static scalar_function_t GetFunction() {
		return floor_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return ceilfloor_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return ceilfloor_get_return_type;
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
