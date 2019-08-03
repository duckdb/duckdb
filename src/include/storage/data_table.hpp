//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/data_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/index_type.hpp"
#include "common/types/data_chunk.hpp"
#include "storage/index.hpp"
#include "storage/table/version_chunk.hpp"
#include "storage/table_statistics.hpp"
#include "storage/block.hpp"
#include "storage/table/column_segment.hpp"
#include "storage/table/persistent_segment.hpp"

#include <atomic>
#include <mutex>
#include <vector>

namespace duckdb {
class ClientContext;
class ColumnDefinition;
class StorageManager;
class TableCatalogEntry;
class Transaction;

struct VersionInfo;

struct TableScanState {
	virtual ~TableScanState() {
	}

	VersionChunk *chunk;
	unique_ptr<ColumnPointer[]> columns;
	index_t offset;
	VersionInfo *version_chain;
	VersionChunk *last_chunk;
	index_t last_chunk_count;
};

struct IndexTableScanState : public TableScanState {
	index_t version_index;
	index_t version_offset;
	vector<unique_ptr<StorageLockKey>> locks;
};

//! DataTable represents a physical table on disk
class DataTable {
public:
	DataTable(StorageManager &storage, string schema, string table, vector<TypeId> types,
	          unique_ptr<vector<unique_ptr<PersistentSegment>>[]> data);

	//! The amount of elements in the table. Note that this number signifies the amount of COMMITTED entries in the
	//! table. It can be inaccurate inside of transactions. More work is needed to properly support that.
	std::atomic<index_t> cardinality;
	//! Total per-tuple size of the table
	index_t tuple_size;
	//! Accumulative per-tuple size
	vector<index_t> accumulative_tuple_size;
	// schema of the table
	string schema;
	// name of the table
	string table;
	//! Types managed by data table
	vector<TypeId> types;
	//! A reference to the base storage manager
	StorageManager &storage;
	//! Indexes
	vector<unique_ptr<Index>> indexes;

public:
	void InitializeScan(TableScanState &state);
	//! Scans up to STANDARD_VECTOR_SIZE elements from the table starting
	// from offset and store them in result. Offset is incremented with how many
	// elements were returned.
	void Scan(Transaction &transaction, DataChunk &result, const vector<column_t> &column_ids,
	          TableScanState &structure);
	//! Fetch data from the specific row identifiers from the base table
	void Fetch(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids, Vector &row_ids);
	//! Append a DataChunk to the table. Throws an exception if the columns
	// don't match the tables' columns.
	void Append(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk);
	//! Delete the entries with the specified row identifier from the table
	void Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_ids);
	//! Update the entries with the specified row identifier from the table
	void Update(TableCatalogEntry &table, ClientContext &context, Vector &row_ids, vector<column_t> &column_ids,
	            DataChunk &data);

	void InitializeIndexScan(IndexTableScanState &state);
	//! Scan used for creating an index, incrementally locks all storage chunks and scans ALL tuples in the table
	//! (including all versions of a tuple)
	void CreateIndexScan(IndexTableScanState &structure, vector<column_t> &column_ids, DataChunk &result);

	VersionChunk *GetChunk(index_t row_number);

	//! Retrieve versioned data for all column_ids of the table
	void RetrieveVersionedData(DataChunk &result, data_ptr_t alternate_version_pointers[],
	                           index_t alternate_version_count);

	//! Retrieves versioned data for a specific set of column_ids of the table
	void RetrieveVersionedData(DataChunk &result, const vector<column_t> &column_ids,
	                           data_ptr_t alternate_version_pointers[], index_t alternate_version_index[],
	                           index_t alternate_version_count);

	//! Add an index to the DataTable
	void AddIndex(unique_ptr<Index> index, vector<unique_ptr<Expression>> &expressions);

private:
	index_t InitializeTable(unique_ptr<vector<unique_ptr<PersistentSegment>>[]> data);
	//! Append a storage chunk with the given start index to the data table. Returns a pointer to the newly created
	//! storage chunk.
	VersionChunk *AppendVersionChunk(index_t start);
	//! Append a subset of a vector to the specified column of the table
	void AppendVector(index_t column, Vector &data, index_t offset, index_t count);

	//! Verify constraints with a chunk from the Append containing all columns of the table
	void VerifyAppendConstraints(TableCatalogEntry &table, DataChunk &chunk);
	//! Verify constraints with a chunk from the Update containing only the specified column_ids
	void VerifyUpdateConstraints(TableCatalogEntry &table, DataChunk &chunk, vector<column_t> &column_ids);

	//! Append a DataChunk to the set of indexes
	void AppendToIndexes(DataChunk &chunk, row_t row_start);
	//! Issue the specified update to the set of indexes
	void UpdateIndexes(TableCatalogEntry &table, vector<column_t> &column_ids, DataChunk &updates,
	                   Vector &row_identifiers);

private:
	//! The stored data of the table
	SegmentTree storage_tree;
	//! The physical columns of the table
	unique_ptr<SegmentTree[]> columns;
};
} // namespace duckdb
