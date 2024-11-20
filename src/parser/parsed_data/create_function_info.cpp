#include "duckdb/parser/parsed_data/create_function_info.hpp"

namespace duckdb {

CreateFunctionInfo::CreateFunctionInfo(CatalogType type, string schema) : CreateInfo(type, std::move(schema)) {
	D_ASSERT(type == CatalogType::SCALAR_FUNCTION_ENTRY || type == CatalogType::AGGREGATE_FUNCTION_ENTRY ||
	         type == CatalogType::TABLE_FUNCTION_ENTRY || type == CatalogType::PRAGMA_FUNCTION_ENTRY ||
	         type == CatalogType::MACRO_ENTRY || type == CatalogType::TABLE_MACRO_ENTRY);
}

void CreateFunctionInfo::CopyFunctionProperties(CreateFunctionInfo &other) const {
	CopyProperties(other);
	other.name = name;
	other.description = description;
	other.parameter_names = parameter_names;
	other.example = example;
}

} // namespace duckdb
