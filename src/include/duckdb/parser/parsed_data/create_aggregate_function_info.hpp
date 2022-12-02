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
	explicit CreateAggregateFunctionInfo(AggregateFunction function);
	explicit CreateAggregateFunctionInfo(AggregateFunctionSet set);

	AggregateFunctionSet functions;

public:
	unique_ptr<CreateInfo> Copy() const override;
};

} // namespace duckdb
