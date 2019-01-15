//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/round.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

void round_function(Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result);
bool round_matches_arguments(vector<TypeId> &arguments);
TypeId round_get_return_type(vector<TypeId> &arguments);

class RoundFunction {
public:
	static const char *GetName() {
		return "round";
	}

	static scalar_function_t GetFunction() {
		return round_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return round_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return round_get_return_type;
	}
};

} // namespace function
} // namespace duckdb
