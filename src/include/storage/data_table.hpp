//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// storage/data_table.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <mutex>
#include <vector>

#include "common/types/data_chunk.hpp"
#include "common/types/statistics.hpp"
#include "common/types/tuple.hpp"

#include "storage/index.hpp"
#include "storage/storage_chunk.hpp"
#include "storage/unique_index.hpp"

namespace duckdb {
class ClientContext;
class ColumnDefinition;
class StorageManager;
class TableCatalogEntry;
class Transaction;

struct VersionInformation;

struct ScanStructure {
	StorageChunk *chunk;
	size_t offset;
	VersionInformation *version_chain;
};

//! DataTable represents a physical table on disk
class DataTable {
	friend class UniqueIndex;

  public:
	DataTable(StorageManager &storage, std::string schema, std::string table,
	          std::vector<TypeId> types);

	void InitializeScan(ScanStructure &structure);
	//! Scans up to STANDARD_VECTOR_SIZE elements from the table starting
	// from offset and store them in result. Offset is incremented with how many
	// elements were returned.
	void Scan(Transaction &transaction, DataChunk &result,
	          const std::vector<column_t> &column_ids,
	          ScanStructure &structure);
	//! Fetch data from the specific row identifiers from the base table
	void Fetch(Transaction &transaction, DataChunk &result,
	           std::vector<column_t> &column_ids, Vector &row_ids);
	//! Append a DataChunk to the table. Throws an exception if the columns
	// don't match the tables' columns.
	void Append(TableCatalogEntry &table, ClientContext &context,
	            DataChunk &chunk);
	//! Delete the entries with the specified row identifier from the table
	void Delete(TableCatalogEntry &table, ClientContext &context,
	            Vector &row_ids);
	//! Update the entries with the specified row identifier from the table
	void Update(TableCatalogEntry &table, ClientContext &context,
	            Vector &row_ids, std::vector<column_t> &column_ids,
	            DataChunk &data);

	//! Scan used for creating an index, incrementally locks all storage chunks
	void CreateIndexScan(ScanStructure &structure,
	                     std::vector<column_t> &column_ids, DataChunk &result);

	//! After finishing the CreateIndexScan, releases all locks acquired during
	//! the scan
	void ReleaseIndexLocks();

	//! Get statistics of the specified column
	Statistics &GetStatistics(column_t oid) {
		if (oid == COLUMN_IDENTIFIER_ROW_ID) {
			return rowid_statistics;
		}
		return statistics[oid];
	}

	std::vector<TypeId> GetTypes(const std::vector<column_t> &column_ids);

	//! Total per-tuple size of the table
	size_t tuple_size;
	//! Accumulative per-tuple size
	std::vector<size_t> accumulative_tuple_size;

	// schema of the table
	std::string schema;
	// name of the table
	std::string table;
	//! Types managed by data table
	std::vector<TypeId> types;

	//! Tuple serializer for this table
	TupleSerializer serializer;
	//! A reference to the base storage manager
	StorageManager &storage;

	StorageChunk *GetChunk(size_t row_number);

	//! Unique indexes
	std::vector<std::unique_ptr<UniqueIndex>> unique_indexes;

	//! Indexes
	std::vector<std::unique_ptr<Index>> indexes;

  private:
	//! Retrieves versioned data from a set of pointers to tuples inside an
	//! UndoBuffer and stores them inside the result chunk; used for scanning of
	//! versioned tuples
	void RetrieveVersionedData(DataChunk &result,
	                           const std::vector<column_t> &column_ids,
	                           uint8_t *alternate_version_pointers[],
	                           size_t alternate_version_index[],
	                           size_t alternate_version_count);
	//! Retrieves data from the base table for use in scans
	void RetrieveBaseTableData(DataChunk &result,
	                           const std::vector<column_t> &column_ids,
	                           sel_t regular_entries[], size_t regular_count,
	                           StorageChunk *current_chunk,
	                           size_t current_offset = 0);

	//! Verify whether or not a new chunk violates any constraints
	void VerifyConstraints(TableCatalogEntry &table, ClientContext &context,
	                       DataChunk &new_chunk);
	//! The stored data of the table
	std::unique_ptr<StorageChunk> chunk_list;
	//! A reference to the last entry in the chunk list
	StorageChunk *tail_chunk;
	//! Row ID statistics
	Statistics rowid_statistics;
	//! The statistics of each of the columns
	std::unique_ptr<Statistics[]> statistics;
	//! Locks used for updating the statistics
	std::unique_ptr<std::mutex[]> statistics_locks;
};
} // namespace duckdb
