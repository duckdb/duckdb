//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

namespace duckdb {

//! An aggregate function in the catalog
class AggregateFunctionCatalogEntry : public StandardEntry {
public:
	AggregateFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateAggregateFunctionInfo *info)
	    : StandardEntry(CatalogType::AGGREGATE_FUNCTION, schema, catalog, info->name),
	      functions(info->functions.functions) {
	}

	//! The aggregate functions
	vector<AggregateFunction> functions;
};
} // namespace duckdb
