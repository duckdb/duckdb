#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

namespace duckdb {

CreateAggregateFunctionInfo::CreateAggregateFunctionInfo(AggregateFunction function)
    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(function.name) {
	name = function.name;
	functions.AddFunction(Move(function));
	internal = true;
}

CreateAggregateFunctionInfo::CreateAggregateFunctionInfo(AggregateFunctionSet set)
    : CreateFunctionInfo(CatalogType::AGGREGATE_FUNCTION_ENTRY), functions(Move(set)) {
	name = functions.name;
	for (auto &func : functions.functions) {
		func.name = functions.name;
	}
	internal = true;
}

unique_ptr<CreateInfo> CreateAggregateFunctionInfo::Copy() const {
	auto result = make_unique<CreateAggregateFunctionInfo>(functions);
	CopyProperties(*result);
	return Move(result);
}

} // namespace duckdb
