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
#include "transaction/transaction.hpp"

namespace duckdb {
class StorageManager;

//! DataTable represents a physical table on disk
class DataTable {
  public:
	DataTable(StorageManager &storage, TableCatalogEntry &table)
	    : storage(storage), size(0), table(table) {}

	void AddColumn(ColumnDefinition &column);
	void AddData(DataChunk &chunk);

	// //! Scans up to STANDARD_VECTOR_SIZE elements from the table starting
	// from offset and store them in result. Offset is incremented with how many
	// elements were returned.
	void Scan(Transaction &transaction, DataChunk &result,
	          const std::vector<size_t> &column_ids, size_t &offset);
	//! Append a DataChunk to the table. Throws an exception if the columns
	//! don't match the tables' columns.
	// void Append(Transaction &transaction, DataChunk &chunk);

	//! Get statistics of the specified column
	Statistics GetStatistics(size_t oid) { return columns[oid]->stats; }

	std::vector<TypeId> GetTypes(const std::vector<size_t> &column_ids);

  private:
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
