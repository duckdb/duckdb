#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"

namespace duckdb {

CreatePragmaFunctionInfo::CreatePragmaFunctionInfo(PragmaFunction function)
    : CreateFunctionInfo(CatalogType::PRAGMA_FUNCTION_ENTRY), functions(function.name) {
	name = function.name;
	functions.AddFunction(move(function));
	internal = true;
}
CreatePragmaFunctionInfo::CreatePragmaFunctionInfo(string name, PragmaFunctionSet functions_p)
    : CreateFunctionInfo(CatalogType::PRAGMA_FUNCTION_ENTRY), functions(move(functions_p)) {
	this->name = move(name);
	internal = true;
}

unique_ptr<CreateInfo> CreatePragmaFunctionInfo::Copy() const {
	auto result = make_unique<CreatePragmaFunctionInfo>(functions.name, functions);
	CopyProperties(*result);
	return move(result);
}

} // namespace duckdb
