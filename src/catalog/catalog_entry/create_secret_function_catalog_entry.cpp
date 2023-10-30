#include "duckdb/catalog/catalog_entry/create_secret_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_secret_function_info.hpp"

namespace duckdb {

CreateSecretFunctionCatalogEntry::CreateSecretFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                       CreateSecretFunctionInfo &info)
    : FunctionEntry(CatalogType::CREATE_SECRET_FUNCTION_ENTRY, catalog, schema, info), functions(std::move(info.functions)) {
}

} // namespace duckdb
