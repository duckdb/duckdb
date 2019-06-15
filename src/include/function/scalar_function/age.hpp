//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/age.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/date.hpp"
#include "common/types/time.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "function/function.hpp"

#include <string.h>

namespace duckdb {

void age_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);
bool age_matches_arguments(vector<SQLType> &arguments);
SQLType age_get_return_type(vector<SQLType> &arguments);

class AgeFunction {
public:
	static const char *GetName() {
		return "age";
	}

	static scalar_function_t GetFunction() {
		return age_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return age_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return age_get_return_type;
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
