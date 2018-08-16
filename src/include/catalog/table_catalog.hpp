//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/table_catalog.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/abstract_catalog.hpp"
#include "catalog/column_catalog.hpp"

#include "common/types/statistics.hpp"

namespace duckdb {

class DataTable;
class SchemaCatalogEntry;

//! A table catalog entry
class TableCatalogEntry : public AbstractCatalogEntry {
  public:
	TableCatalogEntry(Catalog *catalog, std::string name);

	//! A reference to the underlying storage unit used for this table
	DataTable *storage;
	//! A list of columns that are part of this table
	std::vector<std::shared_ptr<ColumnCatalogEntry>> columns;
	//! A map of column name to column index
	std::unordered_map<std::string, size_t> name_map;

	//! Adds a column to this table
	void AddColumn(ColumnCatalogEntry entry);
	//! Returns whether or not a column with the given name exists
	bool ColumnExists(const std::string &name);
	//! Returns the statistics of the oid-th column. Throws an exception if the
	//! access is out of range.
	Statistics GetStatistics(size_t oid);
	//! Returns a reference to the column of the specified name. Throws an
	//! exception if the column does not exist.
	std::shared_ptr<ColumnCatalogEntry> GetColumn(const std::string &name);
	//! Returns a list of types of the table
	std::vector<TypeId> GetTypes();

	virtual std::string ToString() const { return std::string(); }
};
} // namespace duckdb
