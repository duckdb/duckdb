//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/create_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "function/function.hpp"

namespace duckdb {

struct CreateScalarFunctionInfo {
	//! Schema name
	string schema;
	//! Function name
	string name;
	//! Replace function if it already exists instead of failing
	bool or_replace = false;
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

	CreateScalarFunctionInfo() : schema(DEFAULT_SCHEMA), or_replace(false), has_side_effects(false) {
	}
};

} // namespace duckdb
