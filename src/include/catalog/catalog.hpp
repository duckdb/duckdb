//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/catalog.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_set.hpp"

#include "catalog/catalog_entry/list.hpp"

namespace duckdb {

class StorageManager;

//! The Catalog object represents the catalog of the database.
class Catalog {
  public:
	Catalog(StorageManager &storage);

	//! Creates a schema in the catalog.
	void CreateSchema(Transaction &transaction, CreateSchemaInformation *info);
	//! Drops a schema in the catalog.
	void DropSchema(Transaction &transaction, DropSchemaInformation *info);
	//! Creates a table in the catalog.
	void CreateTable(Transaction &transaction, CreateTableInformation *info);
	//! Drops a table from the catalog.
	void DropTable(Transaction &transaction, DropTableInformation *info);

	//! Returns true if the schema exists, and false otherwise.
	bool SchemaExists(Transaction &transaction,
	                  const std::string &name = DEFAULT_SCHEMA);
	//! Returns true if the table exists in the given schema, and false
	//! otherwise.
	bool TableExists(Transaction &transaction, const std::string &schema,
	                 const std::string &table);

	//! Returns a pointer to the schema of the specified name. Throws an
	//! exception if it does not exist.
	SchemaCatalogEntry *GetSchema(Transaction &transaction,
	                              const std::string &name = DEFAULT_SCHEMA);
	//! Returns a pointer to the table in the specified schema. Throws an
	//! exception if the schema or the table does not exist.
	TableCatalogEntry *GetTable(Transaction &transaction,
	                            const std::string &schema,
	                            const std::string &table);

	//! Reference to the storage manager
	StorageManager &storage;

  private:
	//! The catalog set holding the schemas
	CatalogSet schemas;
};
} // namespace duckdb
