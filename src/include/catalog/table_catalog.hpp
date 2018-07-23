
#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/abstract_catalog.hpp"
#include "catalog/column_catalog.hpp"

namespace duckdb {

class DataTable;
class SchemaCatalogEntry;

class TableCatalogEntry : public AbstractCatalogEntry {
  public:
	TableCatalogEntry(Catalog *catalog, std::string name);

	DataTable *storage;
	std::vector<std::shared_ptr<ColumnCatalogEntry>> columns;
	std::unordered_map<std::string, size_t> name_map;

	void AddColumn(ColumnCatalogEntry entry);
	bool ColumnExists(const std::string &name);
	std::shared_ptr<ColumnCatalogEntry> GetColumn(const std::string &name);

	virtual std::string ToString() const { return std::string(); }
};
}
