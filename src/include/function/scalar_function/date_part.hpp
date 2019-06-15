//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/date_part.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {

void date_part_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                        Vector &result);
bool date_part_matches_arguments(vector<SQLType> &arguments);
SQLType date_part_get_return_type(vector<SQLType> &arguments);

class DatePartFunction {
public:
	static const char *GetName() {
		return "date_part";
	}

	static scalar_function_t GetFunction() {
		return date_part_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return date_part_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return date_part_get_return_type;
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
