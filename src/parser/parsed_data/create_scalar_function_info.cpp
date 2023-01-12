#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/alter_function_info.hpp"

namespace duckdb {

CreateScalarFunctionInfo::CreateScalarFunctionInfo(ScalarFunction function)
    : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY), functions(function.name) {
	name = function.name;
	functions.AddFunction(std::move(function));
	internal = true;
}
CreateScalarFunctionInfo::CreateScalarFunctionInfo(ScalarFunctionSet set)
    : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY), functions(std::move(set)) {
	name = functions.name;
	for (auto &func : functions.functions) {
		func.name = functions.name;
	}
	internal = true;
}

unique_ptr<CreateInfo> CreateScalarFunctionInfo::Copy() const {
	ScalarFunctionSet set(name);
	set.functions = functions.functions;
	auto result = make_unique<CreateScalarFunctionInfo>(std::move(set));
	CopyProperties(*result);
	return std::move(result);
}

unique_ptr<AlterInfo> CreateScalarFunctionInfo::GetAlterInfo() const {
	return make_unique_base<AlterInfo, AddFunctionOverloadInfo>(AlterEntryData(catalog, schema, name, true), functions);
}

} // namespace duckdb
