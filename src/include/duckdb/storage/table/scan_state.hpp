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
#include "duckdb/storage/table/segment_lock.hpp"

namespace duckdb {
class ColumnSegment;
class LocalTableStorage;
class CollectionScanState;
class Index;
class RowGroup;
class RowGroupCollection;
class UpdateSegment;
class TableScanState;
class ColumnSegment;
class ValiditySegment;
class TableFilterSet;
class ColumnData;

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
	//! The version of the column data that we are scanning.
	//! This is used to detect if the ColumnData has been changed out from under us during a scan
	//! If this is the case, we re-initialize the scan
	idx_t version;
	//! We initialize one SegmentScanState per segment, however, if scanning a DataChunk requires us to scan over more
	//! than one Segment, we need to keep the scan states of the previous segments around
	vector<unique_ptr<SegmentScanState>> previous_states;

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

class RowGroupScanState {
public:
	RowGroupScanState(CollectionScanState &parent_p)
	    : row_group(nullptr), vector_index(0), max_row(0), parent(parent_p) {
	}

	//! The current row_group we are scanning
	RowGroup *row_group = nullptr;
	//! The vector index within the row_group
	idx_t vector_index = 0;
	//! The maximum row index of this row_group scan
	idx_t max_row = 0;
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
	CollectionScanState(TableScanState &parent_p)
	    : row_group_state(*this), max_row(0), batch_index(0), parent(parent_p) {};

	//! The row_group scan state
	RowGroupScanState row_group_state;
	//! The total maximum row index
	idx_t max_row;
	//! The current batch index
	idx_t batch_index;

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
	TableScanState() : table_state(*this), local_state(*this), table_filters(nullptr) {};

	//! The underlying table scan state
	CollectionScanState table_state;
	//! Transaction-local scan state
	CollectionScanState local_state;

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

struct ParallelCollectionScanState {
	//! The row group collection we are scanning
	RowGroupCollection *collection;
	RowGroup *current_row_group;
	idx_t vector_index;
	idx_t max_row;
	idx_t batch_index;
};

struct ParallelTableScanState {
	//! Parallel scan state for the table
	ParallelCollectionScanState scan_state;
	//! Parallel scan state for the transaction-local state
	ParallelCollectionScanState local_state;
};

class CreateIndexScanState : public TableScanState {
public:
	vector<unique_ptr<StorageLockKey>> locks;
	unique_lock<mutex> append_lock;
	SegmentLock segment_lock;
};

} // namespace duckdb
