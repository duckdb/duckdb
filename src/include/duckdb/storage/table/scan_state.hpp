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

#include "duckdb/execution/adaptive_filter.hpp"

namespace duckdb {
class ColumnSegment;
class LocalTableStorage;
class Index;
class RowGroup;
class UpdateSegment;
class TableScanState;
class ColumnSegment;
class ValiditySegment;
class TableFilterSet;

struct SegmentScanState {
	virtual ~SegmentScanState() {
	}
};

struct IndexScanState {
	virtual ~IndexScanState() {
	}
};

typedef unordered_map<block_id_t, BufferHandle> buffer_handle_set_t;

struct ColumnScanState {
	//! The column segment that is currently being scanned
	ColumnSegment *current = nullptr;
	//! The current row index of the scan
	idx_t row_index = 0;
	//! The internal row index (i.e. the position of the SegmentScanState)
	idx_t internal_index = 0;
	//! Segment scan state
	unique_ptr<SegmentScanState> scan_state;
	//! Child states of the vector
	vector<ColumnScanState> child_states;
	//! Whether or not InitializeState has been called for this segment
	bool initialized = false;
	//! If this segment has already been checked for skipping purposes
	bool segment_checked = false;

public:
	//! Move the scan state forward by "count" rows (including all child states)
	void Next(idx_t count);
	//! Move ONLY this state forward by "count" rows (i.e. not the child states)
	void NextInternal(idx_t count);
	//! Move the scan state forward by STANDARD_VECTOR_SIZE rows
	void NextVector();
};

struct ColumnFetchState {
	//! The set of pinned block handles for this set of fetches
	buffer_handle_set_t handles;
	//! Any child states of the fetch
	vector<unique_ptr<ColumnFetchState>> child_states;

	BufferHandle &GetOrInsertHandle(ColumnSegment &segment);
};

struct LocalScanState {
	~LocalScanState();

	void SetStorage(shared_ptr<LocalTableStorage> storage);
	LocalTableStorage *GetStorage() {
		return storage.get();
	}

	idx_t chunk_index = 0;
	idx_t max_index = 0;
	idx_t last_chunk_count = 0;
	TableFilterSet *table_filters = nullptr;

private:
	shared_ptr<LocalTableStorage> storage;
};

class RowGroupScanState {
public:
	RowGroupScanState(TableScanState &parent_p) : parent(parent_p), vector_index(0), max_row(0) {
	}

	//! The parent scan state
	TableScanState &parent;
	//! The current row_group we are scanning
	RowGroup *row_group = nullptr;
	//! The vector index within the row_group
	idx_t vector_index = 0;
	//! The maximum row index of this row_group scan
	idx_t max_row = 0;
	//! Child column scans
	unique_ptr<ColumnScanState[]> column_scans;
};

class TableScanState {
public:
	TableScanState() : row_group_scan_state(*this), max_row(0) {};

	//! The row_group scan state
	RowGroupScanState row_group_scan_state;
	//! The total maximum row index
	idx_t max_row = 0;
	//! The column identifiers of the scan
	vector<column_t> column_ids;
	//! The table filters (if any)
	TableFilterSet *table_filters = nullptr;
	//! Adaptive filter info (if any)
	unique_ptr<AdaptiveFilter> adaptive_filter;
	//! Transaction-local scan state
	LocalScanState local_state;
};

class CreateIndexScanState : public TableScanState {
public:
	vector<unique_ptr<StorageLockKey>> locks;
	unique_lock<mutex> append_lock;
	unique_lock<mutex> delete_lock;
};

} // namespace duckdb
