//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/create_aggregate_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_function_info.hpp"

namespace duckdb {

struct CreateAggregateFunctionInfo : public CreateFunctionInfo {
	CreateAggregateFunctionInfo() : CreateFunctionInfo(FunctionType::AGGREGATE) {
	}

	//! The hashed aggregate state sizing function
	aggregate_size_t state_size;
	//! The hashed aggregate initialization function
	aggregate_initialize_t initialize;
	//! The hashed aggregate update function
	aggregate_update_t update;
	//! The hashed aggregate finalization function
	aggregate_finalize_t finalize;

	//! The simple aggregate initialization function (may be null)
	aggregate_simple_initialize_t simple_initialize;
	//! The simple aggregate update function (may be null)
	aggregate_simple_update_t simple_update;

	//! Function that gives the return type of the aggregate given the input
	//! arguments
	get_return_type_function_t return_type;

	//! Function that returns true if the arguments need to be cast to the return type
	//! arguments
	matches_argument_function_t cast_arguments;
};

} // namespace duckdb
