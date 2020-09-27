//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class Catalog;
class Constraint;

struct CreateTableFunctionInfo;

//! A table function in the catalog
class TableFunctionCatalogEntry : public StandardEntry {
public:
	TableFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTableFunctionInfo *info);

	//! The table function
	vector<TableFunction> functions;
};
} // namespace duckdb
