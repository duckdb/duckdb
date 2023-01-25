#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"

namespace duckdb {

PragmaFunctionCatalogEntry::PragmaFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                       CreatePragmaFunctionInfo *info)
    : StandardEntry(CatalogType::PRAGMA_FUNCTION_ENTRY, schema, catalog, info->name),
      functions(std::move(info->functions)) {
}

} // namespace duckdb
