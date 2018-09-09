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

#include <atomic>
#include <mutex>
#include <vector>

#include "common/types/data_chunk.hpp"
#include "common/types/statistics.hpp"

#include "storage/storage_chunk.hpp"

namespace duckdb {
class ColumnDefinition;
class StorageManager;
class TableCatalogEntry;
class Transaction;

struct ScanStructure {
	StorageChunk *chunk;
	size_t offset;
};

//! DataTable represents a physical table on disk
class DataTable {
  public:
	DataTable(StorageManager &storage, TableCatalogEntry &table);

	void InitializeScan(ScanStructure &structure);
	//! Scans up to STANDARD_VECTOR_SIZE elements from the table starting
	// from offset and store them in result. Offset is incremented with how many
	// elements were returned.
	void Scan(Transaction &transaction, DataChunk &result, const std::vector<size_t>& column_ids, ScanStructure &structure);
	//! Append a DataChunk to the table. Throws an exception if the columns
	// don't match the tables' columns.
	void Append(Transaction &transaction, DataChunk &chunk);

	//! Get statistics of the specified column
	Statistics& GetStatistics(size_t oid) {
		return statistics[oid];
	}

	std::vector<TypeId> GetTypes(const std::vector<size_t> &column_ids);

  private:
	//! The amount of entries in the table
	size_t count;
	//! A reference to the base storage manager
	StorageManager &storage;
	//! The stored data of the table
	std::unique_ptr<StorageChunk> chunk_list;
	//! A reference to the last entry in the chunk list
	StorageChunk *tail_chunk;
	//! The statistics of each of the columns
	std::unique_ptr<Statistics[]> statistics;
	//! Locks used for updating the statistics
	std::unique_ptr<std::mutex[]> statistics_locks;
	//! A reference to the catalog table entry
	TableCatalogEntry &table;
};
} // namespace duckdb
