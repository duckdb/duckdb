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
#include "storage/table/version_chunk_info.hpp"

namespace duckdb {
class ColumnDefinition;
class DataTable;
class StorageManager;

struct TableScanState;
struct IndexTableScanState;

enum class VersionChunkType : uint8_t { TRANSIENT, PERSISTENT };

class VersionChunk : public SegmentBase {
	friend class VersionChunkInfo;

public:
	VersionChunk(VersionChunkType type, DataTable &table, index_t start);

	//! The type of version chunk
	VersionChunkType type;
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
	//! Get the VersionInfo index for a specific entry
	index_t GetVersionIndex(index_t index);
	//! Get the version info for a specific entry
	VersionInfo *GetVersionInfo(index_t index);
	//! Mark a specified entry in the version chunk as deleted. Requires write lock of the chunk to be held.
	void SetDeleted(index_t index);
	//! Push a number of deleted entries to the undo buffer of the transaction. Requires write lock of the chunk to be
	//! held.
	void PushDeletedEntries(Transaction &transaction, index_t amount);
	//! Push a specific tuple into the undo buffer of the transaction. Requires write lock of the chunk to be held.
	void PushTuple(Transaction &transaction, UndoFlags flag, index_t offset);
	//! Retrieve the tuple data for a specific row identifier. Requires shared lock of the chunk to be held.
	void RetrieveTupleData(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids, index_t offset);
	//! Scan a DataChunk from the version chunk. Returns true if the scanned version_index is the last segment of the
	//! chunk.
	bool Scan(TableScanState &state, Transaction &transaction, DataChunk &result, const vector<column_t> &column_ids,
	          index_t version_index);

	//! Scan used for creating an index, scans ALL tuples in the table (including all versions of a tuple). Returns true
	//! if the chunk is exhausted
	bool CreateIndexScan(IndexTableScanState &state, vector<column_t> &column_ids, DataChunk &result);

	//! Appends the data of a VersionInfo entry to a chunk
	void AppendToChunk(DataChunk &chunk, VersionInfo *info);

	void Update(Vector &row_identifiers, Vector &update_vector, index_t col_idx);

private:
	//! Fetches a single tuple from the base table at rowid row_id, and appends that tuple to the "result" DataChunk
	void RetrieveTupleFromBaseTable(DataChunk &result, vector<column_t> &column_ids, row_t row_id);

	VersionChunkInfo *GetOrCreateVersionInfo(index_t version_index);
	//! Fetch "count" entries from the specified column pointer, and place them in the result vector. The column pointer
	//! is advanced by "count" entries.
	void RetrieveColumnData(ColumnPointer &pointer, Vector &result, index_t count);
	//! Fetch "sel_count" entries from a "count" size chunk of the specified column pointer, where the fetched entries
	//! are chosen by "sel_vector". The column pointer is advanced by "count" entries.
	void RetrieveColumnData(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector,
	                        index_t sel_count);

	void FetchColumnData(TableScanState &state, DataChunk &result, const vector<column_t> &column_ids,
	                     index_t offset_in_chunk, index_t count);
	void FetchColumnData(TableScanState &state, DataChunk &result, const vector<column_t> &column_ids,
	                     index_t offset_in_chunk, index_t scan_count, sel_t sel_vector[], index_t count);
};

} // namespace duckdb
