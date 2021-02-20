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
	explicit CreateAggregateFunctionInfo(AggregateFunction function)
	    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(function.name) {
		this->name = function.name;
		functions.AddFunction(move(function));
	}

	explicit CreateAggregateFunctionInfo(AggregateFunctionSet set)
	    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(move(set)) {
		this->name = functions.name;
		for (auto &func : functions.functions) {
			func.name = functions.name;
		}
	}

	AggregateFunctionSet functions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateAggregateFunctionInfo>(functions);
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
