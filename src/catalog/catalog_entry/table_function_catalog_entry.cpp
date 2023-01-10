#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

TableFunctionCatalogEntry::TableFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                     CreateTableFunctionInfo *info)
    : StandardEntry(CatalogType::TABLE_FUNCTION_ENTRY, schema, catalog, info->name),
      functions(std::move(info->functions)) {
	D_ASSERT(this->functions.Size() > 0);
}

} // namespace duckdb
