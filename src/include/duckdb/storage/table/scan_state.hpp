//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/scan_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include "duckdb/execution/adaptive_filter.hpp"

namespace duckdb {
class LocalTableStorage;
class Index;
class MorselInfo;
class PersistentSegment;
class TransientSegment;

struct IndexScanState {
	virtual ~IndexScanState() {
	}
};

typedef unordered_map<block_id_t, unique_ptr<BufferHandle>> buffer_handle_set_t;

struct ColumnScanState {
	//! The column segment that is currently being scanned
	ColumnSegment *current;
	//! The vector index of the transient segment
	idx_t vector_index;
	//! The primary buffer handle
	unique_ptr<BufferHandle> primary_handle;
	//! The set of pinned block handles for this scan
	buffer_handle_set_t handles;
	//! The locks that are held during the scan, only used by the index scan
	vector<unique_ptr<StorageLockKey>> locks;
	//! Whether or not InitializeState has been called for this segment
	bool initialized = false;
	//! If this segment has already been checked for skipping puorposes
	bool segment_checked = false;

public:
	//! Move on to the next vector in the scan
	void Next();
};

struct ColumnFetchState {
	//! The set of pinned block handles for this set of fetches
	buffer_handle_set_t handles;
};

struct LocalScanState {
	~LocalScanState();

	void SetStorage(LocalTableStorage *storage);
	LocalTableStorage *GetStorage() {
		return storage;
	}

	idx_t chunk_index;
	idx_t max_index;
	idx_t last_chunk_count;

private:
	LocalTableStorage *storage = nullptr;
};

class TableScanState {
public:
	TableScanState(){};
	idx_t current_row, max_row;
	idx_t base_row;
	unique_ptr<ColumnScanState[]> column_scans;
	idx_t column_count;
	unique_ptr<AdaptiveFilter> adaptive_filter;
	LocalScanState local_state;
	MorselInfo *version_info;

	//! Move to the next vector
	void NextVector();
};

class CreateIndexScanState : public TableScanState {
public:
	vector<unique_ptr<StorageLockKey>> locks;
	std::unique_lock<std::mutex> append_lock;
	std::unique_lock<std::mutex> delete_lock;
};

} // namespace duckdb
