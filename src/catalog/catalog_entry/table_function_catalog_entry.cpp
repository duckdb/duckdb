#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;
using namespace std;

TableFunctionCatalogEntry::TableFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                     CreateTableFunctionInfo *info)
    : StandardEntry(CatalogType::TABLE_FUNCTION, schema, catalog, info->name), function(info->function),
      supports_projection(info->supports_projection) {
}
