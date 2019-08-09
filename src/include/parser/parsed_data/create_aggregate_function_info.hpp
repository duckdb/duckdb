//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/create_aggregate_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/create_function_info.hpp"
#include "function/aggregate_function.hpp"

namespace duckdb {

struct CreateAggregateFunctionInfo : public CreateFunctionInfo {
	CreateAggregateFunctionInfo(AggregateFunction function) : CreateFunctionInfo(FunctionType::AGGREGATE), function(function) {
		this->name = function.name;
	}

	AggregateFunction function;
};

} // namespace duckdb
