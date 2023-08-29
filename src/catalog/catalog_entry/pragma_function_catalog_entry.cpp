#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"

namespace duckdb {

PragmaFunctionCatalogEntry::PragmaFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                       CreatePragmaFunctionInfo &info,
                                                       optional_ptr<ClientContext> context)
    : FunctionEntry(CatalogType::PRAGMA_FUNCTION_ENTRY, catalog, schema, info, context),
      functions(std::move(info.functions)) {
}

} // namespace duckdb
