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
#include "duckdb/common/enums/scan_options.hpp"
#include "duckdb/execution/adaptive_filter.hpp"

namespace duckdb {
class ColumnSegment;
class LocalTableStorage;
class CollectionScanState;
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

typedef unordered_map<block_id_t, unique_ptr<BufferHandle>> buffer_handle_set_t;

struct ColumnScanState {
	//! The column segment that is currently being scanned
	ColumnSegment *current;
	//! The current row index of the scan
	idx_t row_index;
	//! The internal row index (i.e. the position of the SegmentScanState)
	idx_t internal_index;
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
	TableFilterSet *table_filters;

private:
	LocalTableStorage *storage = nullptr;
};

class RowGroupScanState {
public:
	RowGroupScanState(CollectionScanState &parent_p) : row_group(nullptr), vector_index(0), max_row(0), parent(parent_p) {
	}

	//! The current row_group we are scanning
	RowGroup *row_group;
	//! The vector index within the row_group
	idx_t vector_index;
	//! The maximum row index of this row_group scan
	idx_t max_row;
	//! Child column scans
	unique_ptr<ColumnScanState[]> column_scans;

public:
	const vector<column_t> &GetColumnIds();
	TableFilterSet *GetFilters();
	AdaptiveFilter *GetAdaptiveFilter();
	idx_t GetParentMaxRow();

private:
	//! The parent scan state
	CollectionScanState &parent;
};

class CollectionScanState {
public:
	CollectionScanState(TableScanState &parent_p) : row_group_state(*this), max_row(0), parent(parent_p) {};

	//! The row_group scan state
	RowGroupScanState row_group_state;
	//! The total maximum row index
	idx_t max_row;

public:
	const vector<column_t> &GetColumnIds();
	TableFilterSet *GetFilters();
	AdaptiveFilter *GetAdaptiveFilter();
	bool Scan(Transaction &transaction, DataChunk &result);
	bool ScanCommitted(DataChunk &result, TableScanType type);

private:
	TableScanState &parent;
};

class TableScanState {
public:
	TableScanState() : table_state(*this), table_filters(nullptr) {};

	//! The underlying table scan state
	CollectionScanState table_state;
	//! Transaction-local scan state
	LocalScanState local_state;

public:
	void Initialize(vector<column_t> column_ids, TableFilterSet *table_filters = nullptr);

	const vector<column_t> &GetColumnIds();
	TableFilterSet *GetFilters();
	AdaptiveFilter *GetAdaptiveFilter();
private:
	//! The column identifiers of the scan
	vector<column_t> column_ids;
	//! The table filters (if any)
	TableFilterSet *table_filters;
	//! Adaptive filter info (if any)
	unique_ptr<AdaptiveFilter> adaptive_filter;
};

class CreateIndexScanState : public TableScanState {
public:
	vector<unique_ptr<StorageLockKey>> locks;
	unique_lock<mutex> append_lock;
	unique_lock<mutex> delete_lock;
};

} // namespace duckdb
