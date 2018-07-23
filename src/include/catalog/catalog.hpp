
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

class Catalog : public AbstractCatalogEntry {
  public:
	Catalog();
	~Catalog();

	void CreateSchema(const std::string &schema = DEFAULT_SCHEMA);
	void CreateTable(const std::string &schema, const std::string &table,
	                 const std::vector<ColumnCatalogEntry> &columns);

	bool SchemaExists(const std::string &name = DEFAULT_SCHEMA);
	bool TableExists(const std::string &schema, const std::string &table);

	std::shared_ptr<SchemaCatalogEntry>
	GetSchema(const std::string &name = DEFAULT_SCHEMA);
	std::shared_ptr<TableCatalogEntry> GetTable(const std::string &schema,
	                                            const std::string &table);

	std::unordered_map<std::string, std::shared_ptr<SchemaCatalogEntry>>
	    schemas;

	std::unique_ptr<StorageManager> storage_manager;

	virtual std::string ToString() const { return std::string(); }
};
}
