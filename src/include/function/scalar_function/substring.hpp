//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/substring.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

void substring_function(Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result);
bool substring_matches_arguments(vector<TypeId> &arguments);
TypeId substring_get_return_type(vector<TypeId> &arguments);

class SubstringFunction {
public:
	static const char *GetName() {
		return "substring";
	}

	static scalar_function_t GetFunction() {
		return substring_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return substring_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return substring_get_return_type;
	}
};

} // namespace function
} // namespace duckdb
