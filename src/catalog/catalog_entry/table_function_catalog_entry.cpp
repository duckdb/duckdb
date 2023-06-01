#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/alter_table_function_info.hpp"

namespace duckdb {

TableFunctionCatalogEntry::TableFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                     CreateTableFunctionInfo *info)
    : StandardEntry(CatalogType::TABLE_FUNCTION_ENTRY, schema, catalog, info->name),
      functions(std::move(info->functions)) {
	D_ASSERT(this->functions.Size() > 0);
}

unique_ptr<CatalogEntry> TableFunctionCatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) {
	if (info->type != AlterType::ALTER_TABLE_FUNCTION) {
		throw InternalException("Attempting to alter TableFunctionCatalogEntry with unsupported alter type");
	}
	auto &function_info = (AlterTableFunctionInfo &)*info;
	if (function_info.alter_table_function_type != AlterTableFunctionType::ADD_FUNCTION_OVERLOADS) {
		throw InternalException(
		    "Attempting to alter TableFunctionCatalogEntry with unsupported alter table function type");
	}
	auto &add_overloads = (AddTableFunctionOverloadInfo &)function_info;

	TableFunctionSet new_set = functions;
	if (!new_set.MergeFunctionSet(add_overloads.new_overloads)) {
		throw BinderException("Failed to add new function overloads to function \"%s\": function already exists", name);
	}
	CreateTableFunctionInfo new_info(std::move(new_set));
	return make_uniq<TableFunctionCatalogEntry>(catalog, schema, &new_info);
}

} // namespace duckdb
