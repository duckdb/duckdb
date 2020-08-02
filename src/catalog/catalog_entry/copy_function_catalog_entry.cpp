#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

CopyFunctionCatalogEntry::CopyFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                   CreateCopyFunctionInfo *info)
    : StandardEntry(CatalogType::COPY_FUNCTION, schema, catalog, info->name), function(info->function) {
}
