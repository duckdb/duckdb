#include "duckdb/parser/parsed_data/create_secret_function_info.hpp"

namespace duckdb {

CreateSecretFunctionInfo::CreateSecretFunctionInfo(CreateSecretFunction function)
    : CreateFunctionInfo(CatalogType::CREATE_SECRET_FUNCTION_ENTRY), functions(function.name) {
	name = function.name;
	functions.AddFunction(std::move(function));
	internal = true;
}
CreateSecretFunctionInfo::CreateSecretFunctionInfo(string name, CreateSecretFunctionSet functions_p)
    : CreateFunctionInfo(CatalogType::CREATE_SECRET_FUNCTION_ENTRY), functions(std::move(functions_p)) {
	this->name = std::move(name);
	internal = true;
}

unique_ptr<CreateInfo> CreateSecretFunctionInfo::Copy() const {
	auto result = make_uniq<CreateSecretFunctionInfo>(functions.name, functions);
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
