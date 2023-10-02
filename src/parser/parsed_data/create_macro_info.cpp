#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreateMacroInfo::CreateMacroInfo(CatalogType type) : CreateFunctionInfo(type, INVALID_SCHEMA) {
}

unique_ptr<CreateInfo> CreateMacroInfo::Copy() const {
	auto result = make_uniq<CreateMacroInfo>(type);
	result->function = function->Copy();
	result->name = name;
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
