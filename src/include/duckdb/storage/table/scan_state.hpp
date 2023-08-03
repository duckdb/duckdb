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
class ColumnSegmentTree;
class ValiditySegment;
class TableFilterSet;
class ColumnData;
class DuckTransaction;
class RowGroupSegmentTree;

struct SegmentScanState {
	virtual ~SegmentScanState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct IndexScanState {
	virtual ~IndexScanState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

typedef unordered_map<block_id_t, BufferHandle> buffer_handle_set_t;

struct ColumnScanState {
	//! The column segment that is currently being scanned
	ColumnSegment *current = nullptr;
	//! Column segment tree
	ColumnSegmentTree *segment_tree = nullptr;
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
	idx_t version = 0;
	//! We initialize one SegmentScanState per segment, however, if scanning a DataChunk requires us to scan over more
	//! than one Segment, we need to keep the scan states of the previous segments around
	vector<unique_ptr<SegmentScanState>> previous_states;
	//! The last read offset in the child state (used for LIST columns only)
	idx_t last_offset = 0;

public:
	void Initialize(const LogicalType &type);
	//! Move the scan state forward by "count" rows (including all child states)
	void Next(idx_t count);
	//! Move ONLY this state forward by "count" rows (i.e. not the child states)
	void NextInternal(idx_t count);
};

struct ColumnFetchState {
	//! The set of pinned block handles for this set of fetches
	buffer_handle_set_t handles;
	//! Any child states of the fetch
	vector<unique_ptr<ColumnFetchState>> child_states;

	BufferHandle &GetOrInsertHandle(ColumnSegment &segment);
};

class CollectionScanState {
public:
	CollectionScanState(TableScanState &parent_p);

	//! The current row_group we are scanning
	RowGroup *row_group;
	//! The vector index within the row_group
	idx_t vector_index;
	//! The maximum row within the row group
	idx_t max_row_group_row;
	//! Child column scans
	unsafe_unique_array<ColumnScanState> column_scans;
	//! Row group segment tree
	RowGroupSegmentTree *row_groups;
	//! The total maximum row index
	idx_t max_row;
	//! The current batch index
	idx_t batch_index;

public:
	void Initialize(const vector<LogicalType> &types);
	const vector<storage_t> &GetColumnIds();
	TableFilterSet *GetFilters();
	AdaptiveFilter *GetAdaptiveFilter();
	bool Scan(DuckTransaction &transaction, DataChunk &result);
	bool ScanCommitted(DataChunk &result, TableScanType type);
	bool ScanCommitted(DataChunk &result, SegmentLock &l, TableScanType type);

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
	void Initialize(vector<storage_t> column_ids, TableFilterSet *table_filters = nullptr);

	const vector<storage_t> &GetColumnIds();
	TableFilterSet *GetFilters();
	AdaptiveFilter *GetAdaptiveFilter();

private:
	//! The column identifiers of the scan
	vector<storage_t> column_ids;
	//! The table filters (if any)
	TableFilterSet *table_filters;
	//! Adaptive filter info (if any)
	unique_ptr<AdaptiveFilter> adaptive_filter;
};

struct ParallelCollectionScanState {
	ParallelCollectionScanState();

	//! The row group collection we are scanning
	RowGroupCollection *collection;
	RowGroup *current_row_group;
	idx_t vector_index;
	idx_t max_row;
	idx_t batch_index;
	atomic<idx_t> processed_rows;
	mutex lock;
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
