
#pragma once

#include <string>
#include <unordered_map>

#include "catalog/abstract_catalog.hpp"
#include "catalog/table_catalog.hpp"

namespace duckdb {

class Catalog;

class SchemaCatalogEntry : public AbstractCatalogEntry {
	friend class Catalog;

  public:
	SchemaCatalogEntry(Catalog *catalog, std::string name);

	bool TableExists(const std::string &table_name);
	std::shared_ptr<TableCatalogEntry> GetTable(const std::string &table);

	std::unordered_map<std::string, std::shared_ptr<TableCatalogEntry>> tables;

	virtual std::string ToString() const { return std::string(); }

  private:
	void CreateTable(const std::string &table_name,
	                 const std::vector<ColumnCatalogEntry> &columns);
};
}
