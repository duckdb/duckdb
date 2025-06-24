#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_function_info.hpp"

namespace duckdb {

CreateTableFunctionInfo::CreateTableFunctionInfo(TableFunction function)
    : CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY), functions(function.name) {
	name = function.name;
	functions.AddFunction(std::move(function));
	internal = true;
}
CreateTableFunctionInfo::CreateTableFunctionInfo(TableFunctionSet set)
    : CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY), functions(std::move(set)) {
	name = functions.name;
	for (auto &func : functions.functions) {
		func.name = functions.name;
	}
	internal = true;
}

unique_ptr<CreateInfo> CreateTableFunctionInfo::Copy() const {
	TableFunctionSet set(name);
	set.functions = functions.functions;
	auto result = make_uniq<CreateTableFunctionInfo>(std::move(set));
	CopyFunctionProperties(*result);
	return std::move(result);
}

unique_ptr<AlterInfo> CreateTableFunctionInfo::GetAlterInfo() const {
	return make_uniq_base<AlterInfo, AddTableFunctionOverloadInfo>(
	    AlterEntryData(catalog, schema, name, OnEntryNotFound::RETURN_NULL), functions);
}

} // namespace duckdb
