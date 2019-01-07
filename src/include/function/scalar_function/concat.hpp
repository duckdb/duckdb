//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/concat.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

void concat_function(Vector inputs[], size_t input_count, Expression &expr, Vector &result);
bool concat_matches_arguments(vector<TypeId> &arguments);
TypeId concat_get_return_type(vector<TypeId> &arguments);

class ConcatFunction {
public:
	static const char *GetName() {
		return "concat";
	}

	static scalar_function_t GetFunction() {
		return concat_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return concat_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return concat_get_return_type;
	}
};

} // namespace function
} // namespace duckdb
