#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/alter_function_info.hpp"

namespace duckdb {

ScalarFunctionCatalogEntry::ScalarFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                       CreateScalarFunctionInfo *info)
    : StandardEntry(CatalogType::SCALAR_FUNCTION_ENTRY, schema, catalog, info->name), functions(info->functions) {
}

unique_ptr<CatalogEntry> ScalarFunctionCatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) {
	if (info->type != AlterType::ALTER_FUNCTION) {
		throw InternalException("Attempting to alter ScalarFunctionCatalogEntry with unsupported alter type");
	}
	auto &function_info = (AlterFunctionInfo &)*info;
	if (function_info.alter_function_type != AlterFunctionType::ADD_FUNCTION_OVERLOADS) {
		throw InternalException("Attempting to alter ScalarFunctionCatalogEntry with unsupported alter function type");
	}
	auto &add_overloads = (AddFunctionOverloadInfo &)function_info;

	ScalarFunctionSet new_set = functions;
	if (!new_set.MergeFunctionSet(add_overloads.new_overloads)) {
		throw BinderException("Failed to add new function overloads to function \"%s\": function already exists", name);
	}
	CreateScalarFunctionInfo new_info(std::move(new_set));
	return make_unique<ScalarFunctionCatalogEntry>(catalog, schema, &new_info);
}

} // namespace duckdb
