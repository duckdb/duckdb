//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

//! A table function in the catalog
class ScalarFunctionCatalogEntry : public StandardEntry {
public:
	ScalarFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateScalarFunctionInfo *info)
	    : StandardEntry(CatalogType::SCALAR_FUNCTION, schema, catalog, info->name), functions(info->functions) {
	}

	//! The scalar functions
	vector<ScalarFunction> functions;
};
} // namespace duckdb
