//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/scan_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/enums/scan_options.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/storage/table/segment_lock.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {
class AdaptiveFilter;
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
class TableFilter;
struct AdaptiveFilterState;
struct TableScanOptions;
struct ScanSamplingInfo;
struct TableFilterState;

struct SegmentScanState {
	virtual ~SegmentScanState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct IndexScanState {
	virtual ~IndexScanState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

typedef unordered_map<block_id_t, BufferHandle> buffer_handle_set_t;

struct ColumnScanState {
	//! The query context for this scan
	QueryContext context;
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
	//! We initialize one SegmentScanState per segment, however, if scanning a DataChunk requires us to scan over more
	//! than one Segment, we need to keep the scan states of the previous segments around
	vector<unique_ptr<SegmentScanState>> previous_states;
	//! The last read offset in the child state (used for LIST columns only)
	idx_t last_offset = 0;
	//! Whether or not we should scan a specific child column
	vector<bool> scan_child_column;
	//! Contains TableScan level config for scanning
	optional_ptr<TableScanOptions> scan_options;

public:
	void Initialize(const QueryContext &context_p, const LogicalType &type, const vector<StorageIndex> &children,
	                optional_ptr<TableScanOptions> options);
	void Initialize(const QueryContext &context_p, const LogicalType &type, optional_ptr<TableScanOptions> options);
	//! Move the scan state forward by "count" rows (including all child states)
	void Next(idx_t count);
	//! Move ONLY this state forward by "count" rows (i.e. not the child states)
	void NextInternal(idx_t count);
};

struct ColumnFetchState {
	//! The query context for this fetch
	QueryContext context;
	//! The set of pinned block handles for this set of fetches
	buffer_handle_set_t handles;
	//! Any child states of the fetch
	vector<unique_ptr<ColumnFetchState>> child_states;

	BufferHandle &GetOrInsertHandle(ColumnSegment &segment);
};

struct ScanFilter {
	ScanFilter(ClientContext &context, idx_t index, const vector<StorageIndex> &column_ids, TableFilter &filter);

	idx_t scan_column_index;
	idx_t table_column_index;
	TableFilter &filter;
	bool always_true;
	unique_ptr<TableFilterState> filter_state;

	bool IsAlwaysTrue() const {
		return always_true;
	}
};

class ScanFilterInfo {
public:
	~ScanFilterInfo();

	void Initialize(ClientContext &context, TableFilterSet &filters, const vector<StorageIndex> &column_ids);

	const vector<ScanFilter> &GetFilterList() const {
		return filter_list;
	}

	optional_ptr<AdaptiveFilter> GetAdaptiveFilter();
	AdaptiveFilterState BeginFilter() const;
	void EndFilter(AdaptiveFilterState state);

	//! Whether or not there is any filter we need to execute
	bool HasFilters() const;

	//! Whether or not there is a filter we need to execute for this column currently
	bool ColumnHasFilters(idx_t col_idx);

	//! Resets any SetFilterAlwaysTrue flags
	void CheckAllFilters();
	//! Labels the filters for this specific column as always true
	//! We do not need to execute them anymore until CheckAllFilters is called
	void SetFilterAlwaysTrue(idx_t filter_idx);

private:
	//! The table filters (if any)
	optional_ptr<TableFilterSet> table_filters;
	//! Adaptive filter info (if any)
	unique_ptr<AdaptiveFilter> adaptive_filter;
	//! The set of filters
	vector<ScanFilter> filter_list;
	//! Whether or not the column has a filter active right now
	unsafe_vector<bool> column_has_filter;
	//! Whether or not the column has a filter active at all
	unsafe_vector<bool> base_column_has_filter;
	//! The amount of filters that are always true currently
	idx_t always_true_filters = 0;
};

enum class OrderByStatistics { MIN, MAX };
enum class RowGroupOrderType { ASC, DESC };
enum class OrderByColumnType { NUMERIC, STRING };

struct CollectionScanStateHelper {
	static Value RetrieveStat(const BaseStatistics &stats, OrderByStatistics order_by, OrderByColumnType column_type);
	static multimap<Value, RowGroup *> OrderRowGroups(RowGroupSegmentTree *row_groups, column_t column_idx,
	                                                  OrderByStatistics order_by, OrderByColumnType column_type);
	static unordered_map<RowGroup *, RowGroup *> CreateRowGroupMap(const multimap<Value, RowGroup *> &row_groups,
	                                                               RowGroupOrderType order_type, RowGroup *&root);
};

class CollectionScanState {
public:
	explicit CollectionScanState(TableScanState &parent_p);
	virtual ~CollectionScanState() = default;

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
	//! The valid selection
	SelectionVector valid_sel;

	RandomEngine random;

public:
	void Initialize(const QueryContext &context, const vector<LogicalType> &types);
	const vector<StorageIndex> &GetColumnIds();
	ScanFilterInfo &GetFilterInfo();
	ScanSamplingInfo &GetSamplingInfo();
	TableScanOptions &GetOptions();
	virtual RowGroup *GetNextRowGroup(RowGroup *row_group);
	virtual RowGroup *GetNextRowGroup(SegmentLock &l, RowGroup *row_group);
	virtual RowGroup *GetRootSegment();
	bool Scan(DuckTransaction &transaction, DataChunk &result);
	bool ScanCommitted(DataChunk &result, TableScanType type);
	bool ScanCommitted(DataChunk &result, SegmentLock &l, TableScanType type);

private:
	TableScanState &parent;
};

class CustomOrderCollectionScanState : public CollectionScanState {
public:
	CustomOrderCollectionScanState(TableScanState &parent_p, const RowGroupOrderOptions &options);
	RowGroup *GetNextRowGroup(RowGroup *row_group) override;
	RowGroup *GetRootSegment() override;

private:
	const column_t column_idx;
	const OrderByStatistics order_by;
	const RowGroupOrderType order_type;
	const OrderByColumnType column_type;

	RowGroup *root;
	unordered_map<RowGroup *, RowGroup *> ordered_row_groups;
};

struct ScanSamplingInfo {
	//! Whether or not to do a system sample during scanning
	bool do_system_sample = false;
	//! The sampling rate to use
	double sample_rate;
};

struct TableScanOptions {
	//! Fetch rows one-at-a-time instead of using the regular scans.
	bool force_fetch_row = false;
};

class CheckpointLock {
public:
	explicit CheckpointLock(unique_ptr<StorageLockKey> lock_p) : lock(std::move(lock_p)) {
	}

private:
	unique_ptr<StorageLockKey> lock;
};

class TableScanState {
public:
	TableScanState();
	~TableScanState();

	//! The underlying table scan state
	unique_ptr<CollectionScanState> table_state;
	//! Transaction-local scan state
	unique_ptr<CollectionScanState> local_state;
	//! Options for scanning
	TableScanOptions options;
	//! Shared lock over the checkpoint to prevent checkpoints while reading
	shared_ptr<CheckpointLock> checkpoint_lock;
	//! Filter info
	ScanFilterInfo filters;
	//! Sampling info
	ScanSamplingInfo sampling_info;

public:
	void Initialize(vector<StorageIndex> column_ids, optional_ptr<ClientContext> context = nullptr,
	                optional_ptr<TableFilterSet> table_filters = nullptr,
	                optional_ptr<SampleOptions> table_sampling = nullptr);

	const vector<StorageIndex> &GetColumnIds();

	ScanFilterInfo &GetFilterInfo();

	ScanSamplingInfo &GetSamplingInfo();

private:
	//! The column identifiers of the scan
	vector<StorageIndex> column_ids;
};

struct ParallelCollectionScanState {
	ParallelCollectionScanState();
	virtual ~ParallelCollectionScanState() = default;
	virtual RowGroup *GetRootSegment(const shared_ptr<RowGroupSegmentTree> &row_groups);
	virtual RowGroup *GetNextRowGroup(const shared_ptr<RowGroupSegmentTree> &row_groups, RowGroup *row_group);

	//! The row group collection we are scanning
	RowGroupCollection *collection;
	RowGroup *current_row_group;
	idx_t vector_index;
	idx_t max_row;
	idx_t batch_index;
	atomic<idx_t> processed_rows;
	mutex lock;
};

class CustomOrderParallelCollectionScanState : public ParallelCollectionScanState {
public:
	explicit CustomOrderParallelCollectionScanState(const RowGroupOrderOptions &options);
	RowGroup *GetRootSegment(const shared_ptr<RowGroupSegmentTree> &row_groups) override;
	RowGroup *GetNextRowGroup(const shared_ptr<RowGroupSegmentTree> &row_groups, RowGroup *row_group) override;

private:
	const column_t column_idx;
	const OrderByStatistics order_by;
	const RowGroupOrderType order_type;
	const OrderByColumnType column_type;

	RowGroup *root;
	unordered_map<RowGroup *, RowGroup *> ordered_row_groups;
};

struct ParallelTableScanState {
	//! Parallel scan state for the table
	unique_ptr<ParallelCollectionScanState> scan_state = make_uniq<ParallelCollectionScanState>();
	//! Parallel scan state for the transaction-local state
	unique_ptr<ParallelCollectionScanState> local_state = make_uniq<ParallelCollectionScanState>();
	//! Shared lock over the checkpoint to prevent checkpoints while reading
	shared_ptr<CheckpointLock> checkpoint_lock;
};

struct PrefetchState {
	~PrefetchState();

	void AddBlock(shared_ptr<BlockHandle> block);

	vector<shared_ptr<BlockHandle>> blocks;
};

class CreateIndexScanState : public TableScanState {
public:
	vector<unique_ptr<StorageLockKey>> locks;
	unique_lock<mutex> append_lock;
	SegmentLock segment_lock;
};

} // namespace duckdb
