//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/version_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/string_heap.hpp"
#include "common/enums/undo_flags.hpp"
#include "storage/storage_lock.hpp"
#include "storage/table/segment_tree.hpp"
#include "storage/table/column_segment.hpp"

namespace duckdb {
class ColumnDefinition;
class DataTable;
class StorageManager;

class VersionChunk;
struct VersionInfo;

struct TableScanState;
struct IndexTableScanState;

struct ColumnPointer {
	//! The column segment
	ColumnSegment *segment;
	//! The offset inside the column segment
	index_t offset;
};

class VersionChunkInfo {
public:
	VersionChunkInfo(VersionChunk &chunk, index_t start);

	//! Whether or not the tuples are deleted
	bool deleted[STANDARD_VECTOR_SIZE] = {0};
	//! The version pointers
	VersionInfo *version_pointers[STANDARD_VECTOR_SIZE] = {nullptr};
	//! The chunk this info belongs to
	VersionChunk &chunk;
	//! The start index
	index_t start;
public:
	// Cleanup the version information of a tuple
	void Cleanup(VersionInfo *info);
	// Undo the changes made by a tuple
	void Undo(VersionInfo *info);
};

class VersionChunk : public SegmentBase {
public:
	VersionChunk(DataTable &table, index_t start);

	//! The table
	DataTable &table;
	//! The version chunk pointers for this version chunk
	// FIXME: replace shared_ptr here with unique_ptr and deletion in transactionmanager
	std::shared_ptr<VersionChunkInfo> version_data[STORAGE_CHUNK_VECTORS];
	//! Pointers to the column segments
	unique_ptr<ColumnPointer[]> columns;
	//! The lock for the storage
	StorageLock lock;
	//! The string heap of the storage chunk
	StringHeap string_heap;

public:
	//! Get a pointer to the row of the specified column
	data_ptr_t GetPointerToRow(index_t col, index_t row);
	//! Get the VersionInfo index for a specific entry
	index_t GetVersionIndex(index_t index);
	//! Get the version info for a specific entry
	VersionInfo *GetVersionInfo(index_t index);
	//! Mark a specified entry in the version chunk as deleted. Requires write lock of the chunk to be held.
	void SetDeleted(index_t index);
	//! Push a number of deleted entries to the undo buffer of the transaction. Requires write lock of the chunk to be held.
	void PushDeletedEntries(Transaction &transaction, index_t amount);
	//! Push a specific tuple into the undo buffer of the transaction. Requires write lock of the chunk to be held.
	void PushTuple(Transaction &transaction, UndoFlags flag, index_t offset);
	//! Retrieve the tuple data for a specific row identifier. Requires shared lock of the chunk to be held.
	void RetrieveTupleData(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids, index_t offset);
	//! Scan a DataChunk from the version chunk. Returns true if the scanned version_index is the last segment of the chunk.
	bool Scan(TableScanState &state, Transaction &transaction, DataChunk &result, const vector<column_t> &column_ids, index_t version_index);

	//! Scan used for creating an index, scans ALL tuples in the table (including all versions of a tuple). Returns true if the chunk is exhausted
	bool CreateIndexScan(IndexTableScanState &state, vector<column_t> &column_ids, DataChunk &result);
public:
	// FIXME: this should not be public!

	//! Retrieves versioned data from a set of pointers to tuples inside an
	//! UndoBuffer and stores them inside the result chunk; used for scanning of
	//! versioned tuples
	void RetrieveVersionedData(DataChunk &result, const vector<column_t> &column_ids,
	                           data_ptr_t alternate_version_pointers[], index_t alternate_version_index[],
	                           index_t alternate_version_count);
	//! Fetches a single tuple from the base table at rowid row_id, and appends that tuple to the "result" DataChunk
	void RetrieveTupleFromBaseTable(DataChunk &result, vector<column_t> &column_ids, row_t row_id);
private:
	VersionChunkInfo* GetOrCreateVersionInfo(index_t version_index);

	//! Fetch "count" entries from the specified column pointer, and place them in the result vector. The column pointer
	//! is advanced by "count" entries.
	void RetrieveColumnData(Vector &result, TypeId type, ColumnPointer &pointer, index_t count);
	//! Fetch "sel_count" entries from a "count" size chunk of the specified column pointer, where the fetched entries
	//! are chosen by "sel_vector". The column pointer is advanced by "count" entries.
	void RetrieveColumnData(Vector &result, TypeId type, ColumnPointer &pointer, index_t count, sel_t *sel_vector,
	                        index_t sel_count);

	void FetchColumnData(TableScanState &state, DataChunk &result, const vector<column_t> &column_ids, index_t offset_in_chunk, index_t count);
	void FetchColumnData(TableScanState &state, DataChunk &result, const vector<column_t> &column_ids, index_t offset_in_chunk, index_t scan_count, sel_t sel_vector[], index_t count);
};

} // namespace duckdb
