//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/create_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_function_info.hpp"

namespace duckdb {

struct CreateScalarFunctionInfo : public CreateFunctionInfo {
	CreateScalarFunctionInfo() : CreateFunctionInfo(FunctionType::SCALAR), has_side_effects(false) {
	}

	//! The main scalar function to execute
	scalar_function_t function;
	//! Function that checks whether or not a set of arguments matches
	matches_argument_function_t matches;
	//! Function that gives the return type of the function given the input
	//! arguments
	get_return_type_function_t return_type;
	//! The bind function (if any)
	bind_scalar_function_t bind;
	// The dependency function (if any)
	dependency_function_t dependency;
	//! Whether or not the function has side effects (e.g. sequence increments, random() functions, NOW()). Functions
	//! with side-effects cannot be constant-folded.
	bool has_side_effects;
};

} // namespace duckdb
