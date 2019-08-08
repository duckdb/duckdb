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
#include "function/scalar_function.hpp"

#include <string.h>

namespace duckdb {

void age_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);
bool age_matches_arguments(vector<SQLType> &arguments);
SQLType age_get_return_type(vector<SQLType> &arguments);

class AgeFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("age", age_matches_arguments, age_get_return_type, age_function);
	}
};

} // namespace duckdb
