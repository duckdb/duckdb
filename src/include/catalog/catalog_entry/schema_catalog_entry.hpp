//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/catalog_entry/schema_catalog_entry.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>

#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "catalog/catalog_entry/table_function_catalog_entry.hpp"

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_set.hpp"

#include "transaction/transaction.hpp"

namespace duckdb {

class Catalog;
class Constraint;

//! A schema in the catalog
class SchemaCatalogEntry : public CatalogEntry {
  public:
	SchemaCatalogEntry(Catalog *catalog, std::string name);

	//! Returns true if a table with the given name exists in the schema
	bool TableExists(Transaction &transaction, const std::string &table_name);
	//! Returns a pointer to a table of the given name. Throws an exception if
	//! the table does not exist.
	TableCatalogEntry *GetTable(Transaction &transaction,
	                            const std::string &table);
	//! Creates a table with the given name in the schema
	void CreateTable(Transaction &transaction, CreateTableInformation *info);
	//! Drops a table with the given name
	void DropTable(Transaction &transaction, DropTableInformation *info);

	//! Gets a table function matching the given function expression
	TableFunctionCatalogEntry *GetTableFunction(Transaction &transaction,
	                                            FunctionExpression *expression);
	//! Create a table function within the given schema
	void CreateTableFunction(Transaction &transaction,
	                         CreateTableFunctionInformation *info);
	//! Drops a table function within the given schema
	void DropTableFunction(Transaction &transaction,
	                       DropTableFunctionInformation *info);

	//! Create a scalar function within the given schema
	void CreateScalarFunction(Transaction &transaction,
	                          CreateScalarFunctionInformation *info);
	//! Gets a scalar function with the given name
	ScalarFunctionCatalogEntry *GetScalarFunction(Transaction &transaction,
	                                              const std::string &name);

	//! Returns true if other objects depend on this object
	virtual bool HasDependents(Transaction &transaction);
	//! Function that drops all dependents (used for Cascade)
	virtual void DropDependents(Transaction &transaction);

	//! The catalog set holding the tables
	CatalogSet tables;
	//! The catalog set holding the table functions
	CatalogSet table_functions;
	//! The catalog set holding the scalar functions
	CatalogSet scalar_functions;
};
} // namespace duckdb
