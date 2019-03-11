//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/length.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

void length_function(Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result);
bool length_matches_arguments(vector<TypeId> &arguments);
TypeId length_get_return_type(vector<TypeId> &arguments);

class LengthFunction {
public:
	static const char *GetName() {
		return "length";
	}

	static scalar_function_t GetFunction() {
		return length_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return length_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return length_get_return_type;
	}
};

} // namespace function
} // namespace duckdb
