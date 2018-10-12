//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/schema_catalog.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>

#include "catalog/abstract_catalog.hpp"
#include "catalog/catalog_set.hpp"
#include "catalog/table_catalog.hpp"

#include "transaction/transaction.hpp"

namespace duckdb {

class Catalog;
class Constraint;

//! A schema in the catalog
class SchemaCatalogEntry : public AbstractCatalogEntry {
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

	//! Returns true if other objects depend on this object
	virtual bool HasDependents(Transaction &transaction);
	//! Function that drops all dependents (used for Cascade)
	virtual void DropDependents(Transaction &transaction);

  private:
	//! The catalog set holding the tables
	CatalogSet tables;
};
} // namespace duckdb
