//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/caseconvert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "common/types/data_chunk.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

void caseconvert_upper_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                BoundFunctionExpression &expr, Vector &result);
void caseconvert_lower_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                BoundFunctionExpression &expr, Vector &result);

bool caseconvert_matches_arguments(vector<SQLType> &arguments);
SQLType caseconvert_get_return_type(vector<SQLType> &arguments);

class UpperFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("upper", caseconvert_matches_arguments, caseconvert_get_return_type, caseconvert_upper_function);
	}
};

class LowerFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("lower", caseconvert_matches_arguments, caseconvert_get_return_type, caseconvert_lower_function);
	}
};

} // namespace duckdb
