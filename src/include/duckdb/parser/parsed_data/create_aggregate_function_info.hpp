//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_aggregate_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CreateAggregateFunctionInfo : public CreateFunctionInfo {
	CreateAggregateFunctionInfo(AggregateFunction function)
	    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(function.name) {
		this->name = function.name;
		functions.AddFunction(move(function));
	}

	CreateAggregateFunctionInfo(AggregateFunctionSet set)
	    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(move(set)) {
		this->name = functions.name;
	}

	AggregateFunctionSet functions;
};

} // namespace duckdb
