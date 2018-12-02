//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// function/scalar_function/date_part.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

void date_part_function(Vector inputs[], size_t input_count, Vector &result);
bool date_part_matches_arguments(std::vector<TypeId> &arguments);
TypeId date_part_get_return_type(std::vector<TypeId> &arguments);

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
};

} // namespace function
} // namespace duckdb
