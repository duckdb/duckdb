//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

//! An aggregate function in the catalog
class AggregateFunctionCatalogEntry : public FunctionEntry {
public:
	static constexpr const CatalogType Type = CatalogType::AGGREGATE_FUNCTION_ENTRY;
	static constexpr const char *Name = "aggregate function";

public:
	AggregateFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateAggregateFunctionInfo &info)
	    : FunctionEntry(CatalogType::AGGREGATE_FUNCTION_ENTRY, catalog, schema, info), functions(info.functions) {
		for (auto &function : functions.functions) {
			function.catalog_name = catalog.GetAttached().GetName();
			function.schema_name = schema.name;
		}
	}

	//! The aggregate functions
	AggregateFunctionSet functions;
};
} // namespace duckdb
