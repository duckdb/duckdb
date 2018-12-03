//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// catalog/catalog_entry/table_function_catalog_entry.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_set.hpp"

#include "transaction/transaction.hpp"

namespace duckdb {

class Catalog;
class Constraint;

//! A table function in the catalog
class TableFunctionCatalogEntry : public CatalogEntry {
  public:
	TableFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
	                          CreateTableFunctionInformation *info);

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! List of return columns
	std::vector<ColumnDefinition> return_values;
	//! A map of return-column name to column index
	std::unordered_map<std::string, column_t> name_map;
	//! Input arguments
	std::vector<TypeId> arguments;
	//! Init function pointer
	table_function_init_t init;
	//! The function pointer
	table_function_t function;
	//! Final function pointer
	table_function_final_t final;

	//! Returns whether or not a column with the given name is returned by the
	//! function
	bool ColumnExists(const std::string &name);
	//! Returns a reference to the column of the specified name. Throws an
	//! exception if the column is not returned by the function.
	ColumnDefinition &GetColumn(const std::string &name);
	//! Returns a list of return-types of the function
	std::vector<TypeId> GetTypes();
};
} // namespace duckdb
