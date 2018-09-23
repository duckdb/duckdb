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
#include "catalog/column_definition.hpp"

#include "common/types/statistics.hpp"

namespace duckdb {

class DataTable;
class SchemaCatalogEntry;

//! A table catalog entry
class TableCatalogEntry : public AbstractCatalogEntry {
  public:
	TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
	                  std::string name,
	                  const std::vector<ColumnDefinition> &columns);

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! A reference to the underlying storage unit used for this table
	std::unique_ptr<DataTable> storage;
	//! A list of columns that are part of this table
	std::vector<ColumnDefinition> columns;
	//! A map of column name to column index
	std::unordered_map<std::string, column_t> name_map;

	//! Adds a column to this table
	// void AddColumn(ColumnDefinition entry);
	//! Returns whether or not a column with the given name exists
	bool ColumnExists(const std::string &name);
	//! Returns the statistics of the oid-th column. Throws an exception if the
	//! access is out of range.
	Statistics &GetStatistics(column_t oid);
	//! Returns a reference to the column of the specified name. Throws an
	//! exception if the column does not exist.
	ColumnDefinition &GetColumn(const std::string &name);
	//! Returns a list of types of the table
	std::vector<TypeId> GetTypes();
};
} // namespace duckdb
