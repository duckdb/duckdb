//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CreateScalarFunctionInfo : public CreateFunctionInfo {
	CreateScalarFunctionInfo(ScalarFunction function) : CreateFunctionInfo(FunctionType::SCALAR) {
		this->name = function.name;
		functions.push_back(function);
	}
	CreateScalarFunctionInfo(ScalarFunctionSet set)
	    : CreateFunctionInfo(FunctionType::SCALAR), functions(move(set.functions)) {
		this->name = set.name;
	}

	vector<ScalarFunction> functions;
};

} // namespace duckdb
