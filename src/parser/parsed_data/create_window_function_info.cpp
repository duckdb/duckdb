#include "duckdb/parser/parsed_data/create_window_function_info.hpp"

namespace duckdb {

CreateWindowFunctionInfo::CreateWindowFunctionInfo(WindowFunction function)
    : CreateFunctionInfo(CatalogType::WINDOW_FUNCTION_ENTRY), functions(function.name) {
	name = function.name;
	functions.AddFunction(std::move(function));
	internal = true;
}

CreateWindowFunctionInfo::CreateWindowFunctionInfo(WindowFunctionSet set)
    : CreateFunctionInfo(CatalogType::WINDOW_FUNCTION_ENTRY), functions(std::move(set)) {
	name = functions.name;
	for (auto &func : functions.functions) {
		func.name = functions.name;
	}
	internal = true;
}

unique_ptr<CreateInfo> CreateWindowFunctionInfo::Copy() const {
	auto result = make_uniq<CreateWindowFunctionInfo>(functions);
	CopyFunctionProperties(*result);
	return std::move(result);
}

} // namespace duckdb
