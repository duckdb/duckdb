#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

CreateTableFunctionInfo::CreateTableFunctionInfo(TableFunction function)
	: CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY), functions(function.name) {
	name = function.name;
	functions.AddFunction(move(function));
	internal = true;
}
CreateTableFunctionInfo::CreateTableFunctionInfo(TableFunctionSet set)
	: CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY), functions(move(set)) {
	name = functions.name;
	for (auto &func : functions.functions) {
		func.name = functions.name;
	}
	internal = true;
}

unique_ptr<CreateInfo> CreateTableFunctionInfo::Copy() const {
	TableFunctionSet set(name);
	set.functions = functions.functions;
	auto result = make_unique<CreateTableFunctionInfo>(move(set));
	CopyProperties(*result);
	return move(result);
}

}
