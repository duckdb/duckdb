//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class Catalog;
class Constraint;

struct CreateTableFunctionInfo;

//! A table function in the catalog
class TableFunctionCatalogEntry : public StandardEntry {
public:
	TableFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTableFunctionInfo *info);

	//! The table function
	TableFunction function;
	//! A map of return-column name to column index
	unordered_map<string, column_t> name_map;

	//! Returns whether or not a column with the given name is returned by the
	//! function
	bool ColumnExists(const string &name);
	//! Returns a list of return-types of the function
	vector<TypeId> GetTypes();
};
} // namespace duckdb
