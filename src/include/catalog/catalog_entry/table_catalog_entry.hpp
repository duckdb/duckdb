//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// catalog/catalog_entry/table_catalog_entry.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog_entry.hpp"
#include "parser/column_definition.hpp"

#include "common/types/statistics.hpp"

#include "parser/constraint.hpp"
#include "parser/parsed_data.hpp"

namespace duckdb {

class DataTable;
class SchemaCatalogEntry;

//! A table catalog entry
class TableCatalogEntry : public CatalogEntry {
  public:
	//! Create a real TableCatalogEntry and initialize storage for it
	TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
	                  CreateTableInformation *info);
	TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
	                  CreateTableInformation *info,
	                  std::shared_ptr<DataTable> storage);

	//! The schema the table belongs to
	SchemaCatalogEntry *schema;
	//! A reference to the underlying storage unit used for this table
	std::shared_ptr<DataTable> storage;
	//! A list of columns that are part of this table
	std::vector<ColumnDefinition> columns;
	//! A list of constraints that are part of this table
	std::vector<std::unique_ptr<Constraint>> constraints;
	//! A map of column name to column index
	std::unordered_map<std::string, column_t> name_map;

	std::unique_ptr<CatalogEntry> AlterEntry(AlterInformation *info);
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

  private:
	void Initialize(CreateTableInformation *info);
};
} // namespace duckdb
