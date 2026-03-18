#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_window_function_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

WindowFunctionCatalogEntry::WindowFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                       CreateWindowFunctionInfo &info)
    : FunctionEntry(Type, catalog, schema, info), functions(info.functions) {
	for (auto &function : functions.functions) {
		function.catalog_name = catalog.GetAttached().GetName();
		function.schema_name = schema.name;
	}
}

} // namespace duckdb
