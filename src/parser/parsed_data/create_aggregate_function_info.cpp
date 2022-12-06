#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

namespace duckdb {

CreateAggregateFunctionInfo::CreateAggregateFunctionInfo(AggregateFunction function)
    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(function.name) {
	name = function.name;
	functions.AddFunction(move(function));
	internal = true;
}

CreateAggregateFunctionInfo::CreateAggregateFunctionInfo(AggregateFunctionSet set)
    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(move(set)) {
	name = functions.name;
	for (auto &func : functions.functions) {
		func.name = functions.name;
	}
	internal = true;
}

unique_ptr<CreateInfo> CreateAggregateFunctionInfo::Copy() const {
	auto result = make_unique<CreateAggregateFunctionInfo>(functions);
	CopyProperties(*result);
	return move(result);
}

} // namespace duckdb
