
#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/abstract_catalog.hpp"
#include "catalog/column_catalog.hpp"

namespace duckdb {

class SchemaCatalogEntry;

class TableCatalogEntry : public AbstractCatalogEntry {
  public:
	TableCatalogEntry(std::string name);

	uint64_t size;
	std::unordered_map<std::string, std::shared_ptr<ColumnCatalogEntry>>
	    columns;

	void AddColumn(ColumnCatalogEntry entry);
	bool ColumnExists(const std::string &name);

	std::weak_ptr<SchemaCatalogEntry> parent;

	virtual std::string ToString() const { return std::string(); }
};
}