//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/aggregate_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_set.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_aggregate_function_info.hpp"
#include "transaction/transaction.hpp"

namespace duckdb {

class SchemaCatalogEntry;

//! An aggregate function in the catalog
class AggregateFunctionCatalogEntry : public CatalogEntry {
public:
	AggregateFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateAggregateFunctionInfo *info)
	    : CatalogEntry(CatalogType::AGGREGATE_FUNCTION, catalog, info->name), schema(schema),
	      functions(info->functions.functions) {
	}

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! The aggregate functions
	vector<AggregateFunction> functions;
};
} // namespace duckdb
