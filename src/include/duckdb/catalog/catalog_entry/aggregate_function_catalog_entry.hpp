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
#include "duckdb/function/function_set.hpp"

namespace duckdb {
struct CreateAggregateFunctionInfo;

//! An aggregate function in the catalog
class AggregateFunctionCatalogEntry : public FunctionEntry {
public:
	static constexpr const CatalogType Type = CatalogType::AGGREGATE_FUNCTION_ENTRY;
	static constexpr const char *Name = "aggregate function";

public:
	AggregateFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateAggregateFunctionInfo &info);

	bool IsRegisteredFunction(const shared_ptr<const AggregateFunction> &function) const;

	//! The aggregate functions
	AggregateFunctionSet functions;

private:
	//! The aggregate functions installed when the catalog entry was created
	vector<shared_ptr<const AggregateFunction>> registered_functions;
};
} // namespace duckdb
