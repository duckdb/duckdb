//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

class Catalog;
struct CreatePragmaFunctionInfo;

//! A table function in the catalog
class PragmaFunctionCatalogEntry : public StandardEntry {
public:
	PragmaFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreatePragmaFunctionInfo *info);

	//! The pragma functions
	PragmaFunctionSet functions;
};
} // namespace duckdb
