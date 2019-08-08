//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/create_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_function_info.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

struct CreateScalarFunctionInfo : public CreateFunctionInfo {
	CreateScalarFunctionInfo(ScalarFunction function) : CreateFunctionInfo(FunctionType::SCALAR), function(function) {
		this->name = function.name;
	}

	ScalarFunction function;
};

} // namespace duckdb
