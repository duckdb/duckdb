//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// function/scalar_function/abs.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

void abs_function(Vector inputs[], size_t input_count, Vector &result);
bool abs_matches_arguments(std::vector<TypeId> &arguments);
TypeId abs_get_return_type(std::vector<TypeId> &arguments);

class AbsFunction {
  public:
	static const char *GetName() {
		return "abs";
	}

	static scalar_function_t GetFunction() {
		return abs_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return abs_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return abs_get_return_type;
	}
};

} // namespace function
} // namespace duckdb