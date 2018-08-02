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

#include <memory>
#include <string>
#include <unordered_map>

#include "catalog/abstract_catalog.hpp"
#include "catalog/column_catalog.hpp"
#include "catalog/schema_catalog.hpp"
#include "catalog/table_catalog.hpp"

namespace duckdb {

#define DEFAULT_SCHEMA ""

class StorageManager;

//! The Catalog object represents the catalog of the database.
class Catalog : public AbstractCatalogEntry {
  public:
	Catalog();
	~Catalog();

	//! Creates a schema in the catalog. Throws an exception if it already
	//! exists.
	void CreateSchema(const std::string &schema = DEFAULT_SCHEMA);
	//! Creates a table in the specified schema with the specified set of
	//! columns. Throws an exception if it already exists.
	void CreateTable(const std::string &schema, const std::string &table,
	                 const std::vector<ColumnCatalogEntry> &columns);

	//! Returns true if the schema exists, and false otherwise.
	bool SchemaExists(const std::string &name = DEFAULT_SCHEMA);
	//! Returns true if the table exists in the given schema, and false
	//! otherwise.
	bool TableExists(const std::string &schema, const std::string &table);

	//! Returns a reference to the schema of the specified name. Throws an
	//! exception if it does not exist.
	std::shared_ptr<SchemaCatalogEntry>
	GetSchema(const std::string &name = DEFAULT_SCHEMA);
	//! Returns a reference to the table in the specified schema. Throws an
	//! exception if the table does not exist.
	std::shared_ptr<TableCatalogEntry> GetTable(const std::string &schema,
	                                            const std::string &table);

	//! The set of schemas present in the catalog.
	std::unordered_map<std::string, std::shared_ptr<SchemaCatalogEntry>>
	    schemas;

	//! The underlying storage manager that manages physical storage on disk
	std::unique_ptr<StorageManager> storage_manager;

	virtual std::string ToString() const { return std::string(); }
};
} // namespace duckdb
