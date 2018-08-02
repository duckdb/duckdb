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
#include "catalog/table_catalog.hpp"

namespace duckdb {

class Catalog;

//! A schema in the catalog
class SchemaCatalogEntry : public AbstractCatalogEntry {
	friend class Catalog;

  public:
	SchemaCatalogEntry(Catalog *catalog, std::string name);

	//! Returns true if a table with the given name exists in the schema
	bool TableExists(const std::string &table_name);
	//! Returns a reference to a table of the given name. Throws an exception if
	//! the table does not exist.
	std::shared_ptr<TableCatalogEntry> GetTable(const std::string &table);

	//! The set of tables contained in the schema
	std::unordered_map<std::string, std::shared_ptr<TableCatalogEntry>> tables;

	virtual std::string ToString() const { return std::string(); }

  private:
	//! Creates a table with the given name and the given set of columns in the
	//! schema.
	void CreateTable(const std::string &table_name,
	                 const std::vector<ColumnCatalogEntry> &columns);
};
} // namespace duckdb
