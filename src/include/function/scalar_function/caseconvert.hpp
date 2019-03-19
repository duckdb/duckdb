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
namespace function {

void caseconvert_upper_function(Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result);
void caseconvert_lower_function(Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result);

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
};

} // namespace function
} // namespace duckdb
