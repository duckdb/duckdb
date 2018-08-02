//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// storage/data_table.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/types/data_chunk.hpp"

#include "storage/data_column.hpp"

namespace duckdb {
class StorageManager;

//! DataTable represents a physical table on disk
class DataTable {
  public:
	DataTable(StorageManager &storage, TableCatalogEntry &table)
	    : storage(storage), size(0), table(table) {}

	//! Add a new column to the table, filling it with its default value if the
	//! table is not empty
	void AddColumn(ColumnCatalogEntry &column);

	//! Append a DataChunk to the table. Throws an exception if the columns
	//! don't match the tables' columns.
	void AddData(DataChunk &chunk);

	//! The amount of entries in the table
	size_t size;
	//! A reference to the base storage manager
	StorageManager &storage;
	//! The physical columns owned by this table
	std::vector<std::unique_ptr<DataColumn>> columns;
	//! A reference to the catalog table entry
	TableCatalogEntry &table;
};
} // namespace duckdb
