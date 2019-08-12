//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/catalog_entry/scalar_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_set.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "transaction/transaction.hpp"

namespace duckdb {

class SchemaCatalogEntry;

//! A table function in the catalog
class ScalarFunctionCatalogEntry : public CatalogEntry {
public:
	ScalarFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateScalarFunctionInfo *info)
	    : CatalogEntry(CatalogType::SCALAR_FUNCTION, catalog, info->name), schema(schema), functions(info->functions) {
	}

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! The scalar functions
	vector<ScalarFunction> functions;
};
} // namespace duckdb
