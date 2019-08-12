//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/function_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/scalar_function.hpp"

namespace duckdb {

class FunctionSet {
public:
	FunctionSet(string name);

	//! The name of the function set
	string name;
	//! The set of functions
	vector<ScalarFunction> functions;
public:
	void AddFunction(ScalarFunction function);
};

} // namespace duckdb
