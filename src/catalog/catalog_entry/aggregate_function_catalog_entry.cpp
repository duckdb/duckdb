#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

AggregateFunctionCatalogEntry::AggregateFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                             CreateAggregateFunctionInfo &info)
    : FunctionEntry(CatalogType::AGGREGATE_FUNCTION_ENTRY, catalog, schema, info), functions(info.functions) {
	for (auto &function : functions.functions) {
		function.catalog_name = catalog.GetAttached().GetName();
		function.schema_name = schema.name;
	}
}

} // namespace duckdb
