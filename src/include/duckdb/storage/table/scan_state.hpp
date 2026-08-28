//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/scan_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/storage/table/segment_lock.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {
class AdaptiveFilter;
class AsyncTask;
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
class ClientContext;
class DatabaseInstance;
struct AdaptiveFilterState;
struct TableScanOptions;
struct ScanSamplingInfo;
struct TableFilterState;
template <class T>
struct SegmentNode;

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

struct PushedDownExpressionState {
public:
	explicit PushedDownExpressionState(ClientContext &context) : executor(context) {
	}

public:
	//! The executor to execute the expression
	ExpressionExecutor executor;
	//! The pushed down expression to execute
	unique_ptr<Expression> expression;
	//! The target chunk to store the result of the execution
	DataChunk target;
	DataChunk input;
};

struct ColumnScanState {
	explicit ColumnScanState(optional_ptr<CollectionScanState> parent_p) : parent(parent_p) {
	}

	optional_ptr<CollectionScanState> parent;
	//! The query context for this scan
	QueryContext context;
	//! The column segment that is currently being scanned
	optional_ptr<SegmentNode<ColumnSegment>> current;
	//! Column segment tree
	ColumnSegmentTree *segment_tree = nullptr;
	//! The current row offset in the column
	idx_t offset_in_column = 0;
	//! The internal row index (i.e. the position of the SegmentScanState)
	idx_t internal_index = 0;
	//! Storage index of the current column that's being scanned
	StorageIndex storage_index;
	//! Segment scan state
	unique_ptr<SegmentScanState> scan_state;
	//! Child states of the vector
	unsafe_vector<ColumnScanState> child_states;
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
	//! (optionally) the expression state for any pushed down expression(s)
	unique_ptr<PushedDownExpressionState> expression_state;
	//! Whether or not updates should be allowed
	UpdateScanType update_scan_type = UpdateScanType::STANDARD;

public:
	void PushDownCast(const LogicalType &original_type, const LogicalType &cast_type);

public:
	void Initialize(const QueryContext &context_p, const LogicalType &type, const StorageIndex &column_id,
	                optional_ptr<TableScanOptions> options);
	void Initialize(const QueryContext &context_p, const LogicalType &type, optional_ptr<TableScanOptions> options);
	//! Move the scan state forward by "count" rows (including all child states)
	void Next(idx_t count);
	//! Move ONLY this state forward by "count" rows (i.e. not the child states)
	void NextInternal(idx_t count);
	//! Returns the current row position in the segment
	idx_t GetPositionInSegment() const;
};

enum class FetchType {
	//! Verify if each row is valid for the transaction prior to fetching
	TRANSACTIONAL_FETCH,
	// Force fetch the row, regardless of it if is valid for the transaction or not
	FORCE_FETCH
};

struct ColumnFetchState {
	FetchType fetch_type = FetchType::TRANSACTIONAL_FETCH;
	//! The query context for this fetch
	QueryContext context;
	//! The set of pinned block handles for this set of fetches
	buffer_handle_set_t handles;
	//! Any child states of the fetch
	vector<unique_ptr<ColumnFetchState>> child_states;
	//! The current row group we are fetching from
	optional_ptr<SegmentNode<RowGroup>> row_group;

	BufferHandle &GetOrInsertHandle(ColumnSegment &segment);
};

struct ScanFilter {
	ScanFilter(ClientContext &context, ProjectionIndex index, const vector<StorageIndex> &column_ids,
	           TableFilter &filter);

	ProjectionIndex scan_column_index;
	StorageIndex table_column_index;
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
	optional_ptr<const TableFilterSet> GetTableFilters() const {
		return table_filters.get();
	}
	optional_ptr<const vector<StorageIndex>> GetColumnIds() const {
		return column_ids;
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
	//! Maps scan projection indexes to storage column indexes
	optional_ptr<const vector<StorageIndex>> column_ids;
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

enum class VectorPrepareState : uint8_t {
	//! No vector is currently prepared for processing
	NONE,
	//! A vector is prepared for processing
	PREPARED,
	//! A vector is prepared and its I/O has been registered
	IO_REGISTERED
};

//! Eligibility state of one vector, computed by RowGroup::PrepareScan and consumed by ProcessPreparedScan
struct PreparedScanVector {
	PreparedScanVector();

	//! The prepare state of the current vector
	VectorPrepareState prepare_state = VectorPrepareState::NONE;
	//! The number of rows in the prepared vector
	idx_t max_count = 0;
	//! The number of rows visible to the transaction (held in CollectionScanState::valid_sel)
	idx_t visible_count = 0;
	//! Whether the prepared vector has a system sample selection
	bool has_sample_selection = false;
	//! The number of sampled rows (held in sample_sel)
	idx_t sample_count = 0;
	//! The system sample selection
	SelectionVector sample_sel;

	void Reset();
};

//! Batch size predictor for sub-vector scanning.
//!
//! Goal: given a byte budget (scan_target_size_bytes), decide how many rows to scan per batch so that the
//! materialized chunk never exceeds the budget — no matter where in the row group the batch lands.
//!
//! The batch size is derived purely from the worst-case row width computed from row group statistics
//! (lazily, on the first PredictBatchSize call). `initialized` is cleared at each row group boundary so
//! the next call re-computes it, since string length distributions vary across row groups.
struct ScanSizePredictor {
	//! Fallback per-row estimate for columns with no usable statistics and for variable-size types whose
	//! width cannot be bounded (LIST/MAP have no length statistic). Only an estimate, not a real bound.
	static constexpr double DEFAULT_BYTES_PER_ROW = 256.0;

	//! Worst-case per-row byte bound from row group statistics: fixed column widths + per-VARCHAR
	//! (string_t slot + MaxStringLength). Every row's string is <= MaxStringLength by definition, so a
	//! batch of target_bytes / worst_case_bytes_per_row rows can never exceed the budget — even if it
	//! lands entirely on the largest blobs. Computed at cold start; 0 = not computed (no bound applied).
	double worst_case_bytes_per_row = 0;
	bool initialized = false;

	//! Diagnostic counters — accumulated across batches within a scan
	idx_t total_batches = 0;
	//! Batches where the byte bound was tighter than the rows remaining in the vector, i.e. the batch was
	//! actually shrunk below a full (remaining) vector read.
	idx_t total_safe_clamped_batches = 0;
	//! Rows that fit the budget at the worst-case width on the most recent call, for per-batch TRACE
	//! logging at the scan call site. The returned scan_count is min(this, max_rows).
	idx_t last_safe_rows = 0;

	//! Invalidate the cold-start estimate so the next PredictBatchSize re-computes it (e.g. at a row group
	//! boundary). Diagnostic counters accumulate across the whole scan and are intentionally left untouched.
	void Reset() {
		initialized = false;
	}

	//! Predict batch size for a multi-column scan, clamped to [1, max_rows].
	//! Lazily computes the worst-case per-row width from row group statistics on the first call.
	idx_t PredictBatchSize(idx_t target_bytes, idx_t max_rows, const vector<StorageIndex> &column_ids,
	                       RowGroup &row_group);
	//! Single-column variant for the checkpoint read path (one ColumnData, possibly nested, scanned into a
	//! single Vector). Cold-starts from the column's own statistics instead of a row group.
	idx_t PredictBatchSizeSingle(idx_t target_bytes, idx_t max_rows, const LogicalType &type,
	                             optional_ptr<BaseStatistics> stats);

	//! Emit the accumulated diagnostic counters as a single TRACE line at scan completion. No-op if no batch ran.
	void LogStats(ClientContext &context) const;
	//! Emit the detail of the most recent PredictBatchSize call (byte bound vs final scan_count) as a TRACE
	//! line. No-op when context is not a valid session (e.g. checkpoint / WAL paths).
	void LogBatch(optional_ptr<ClientContext> context, idx_t target_bytes, idx_t max_rows, idx_t scan_count) const;

private:
	//! Rows that fit target_bytes at the worst-case row width, clamped to [1, max_rows]. Shared by both
	//! predict entry points so they are provably bounded by the same formula.
	idx_t ApplyBudget(idx_t target_bytes, idx_t max_rows);
};

//! Tracks progress within a single vector during sub-batch scanning.
//! A standard 2048-row vector is split into multiple smaller batches,
//! each sized by ScanSizePredictor to stay within the byte budget.
struct SubVectorScanState {
public:
	//! Rows of the current vector not yet consumed by prior batches
	idx_t RemainingRows() const {
		return vector_max_count - offset;
	}

	void Reset() {
		offset = 0;
		vector_max_count = 0;
		valid_count = 0;
		valid_sel_cursor = 0;
	}

	//! Start scanning a fresh vector: record its total/surviving row count and rewind to the start
	void BeginVector(idx_t max_count, idx_t valid_count_p) {
		offset = 0;
		vector_max_count = max_count;
		valid_count = valid_count_p;
		valid_sel_cursor = 0;
	}

	//! Advance past the batch just scanned; returns true once the whole vector has been consumed
	bool Advance(idx_t scan_count) {
		offset += scan_count;
		return offset >= vector_max_count;
	}

	bool InProgress() const {
		return offset > 0 && offset < vector_max_count;
	}

	//! Window a vector-wide valid selection to the current batch [offset, offset + scan_count) and rebase the
	//! surviving absolute positions to [0, scan_count) so they index into the batch-sized result. Advances
	//! valid_sel_cursor past the consumed entries and returns the number of survivors in this batch.
	idx_t WindowValidSelection(const SelectionVector &valid_sel, idx_t scan_count, SelectionVector &batch_sel) {
		idx_t batch_end = offset + scan_count;
		idx_t survivors = 0;
		while (valid_sel_cursor < valid_count && valid_sel.get_index(valid_sel_cursor) < batch_end) {
			idx_t abs_pos = valid_sel.get_index(valid_sel_cursor++);
			batch_sel.set_index(survivors++, abs_pos - offset);
		}
		return survivors;
	}

	static bool IsActive(idx_t scan_target_size_bytes) {
		return scan_target_size_bytes > 0;
	}

private:
	//! Current row offset within the vector (0..vector_max_count)
	idx_t offset = 0;
	//! Total scannable rows in the current vector (STANDARD_VECTOR_SIZE for a full vector, less for the tail)
	idx_t vector_max_count = 0;
	//! Surviving (non-deleted) rows in the current vector; equals vector_max_count when there are no deletes
	idx_t valid_count = 0;
	//! Entries of valid_sel already consumed by prior batches of the current vector (delete sub-batch cursor)
	idx_t valid_sel_cursor = 0;
};

class CollectionScanState {
public:
	explicit CollectionScanState(TableScanState &parent_p);
	//! The query context for this scan
	QueryContext context;
	//! The current row_group we are scanning
	optional_ptr<SegmentNode<RowGroup>> row_group;
	//! The vector index within the row_group
	idx_t vector_index;
	//! The maximum row within the row group
	idx_t max_row_group_row;
	//! Child column scans
	unsafe_vector<ColumnScanState> column_scans;
	//! Row group segment tree we are scanning
	shared_ptr<RowGroupSegmentTree> row_groups;
	//! The total maximum row index
	idx_t max_row;
	//! The current batch index
	idx_t batch_index;
	//! The row_number base for the current batch (number of committed rows before this batch)
	//! Only set when the row_number virtual column is being scanned
	optional_idx row_number_base;
	//! The valid selection
	SelectionVector valid_sel;
	//! The currently prepared vector (see RowGroup::PrepareScan)
	PreparedScanVector prepared_vector;

	RandomEngine random;

	//! The amount of tuples considered by a scan, before applying filters
	idx_t rows_scanned = 0;

	//! Optional state for custom row group ordering
	unique_ptr<RowGroupReorderer> reorderer;

	//! Sub-vector scan state for controlling per-batch row count
	SubVectorScanState sub_vector_state;
	//! Predictor for adaptive sub-vector batch sizing
	ScanSizePredictor size_predictor;

public:
	void Initialize(const QueryContext &context_p, const vector<LogicalType> &types);
	const vector<StorageIndex> &GetColumnIds();
	ScanFilterInfo &GetFilterInfo();
	ScanSamplingInfo &GetSamplingInfo();
	TableScanOptions &GetOptions();
	optional_ptr<SegmentNode<RowGroup>> GetNextRowGroup(SegmentNode<RowGroup> &row_group) const;
	optional_ptr<SegmentNode<RowGroup>> GetNextRowGroup(SegmentLock &l, SegmentNode<RowGroup> &row_group) const;
	optional_ptr<SegmentNode<RowGroup>> GetRootSegment() const;
	bool Scan(DuckTransaction &transaction, DataChunk &result);
	bool Scan(DataChunk &result, TableScanType type, optional_ptr<SegmentLock> l = nullptr);
	//! Prepares the next eligible vector of the assignment and collects its I/O tasks
	bool PrepareScanIO(DuckTransaction &transaction, vector<unique_ptr<AsyncTask>> &tasks);
	//! Processes the vector prepared by PrepareScanIO
	void ProcessPreparedScan(DuckTransaction &transaction, DataChunk &result);

private:
	TableScanState &parent;
};

struct ScanSamplingInfo {
	//! Whether or not to do a system sample during scanning
	bool do_system_sample = false;
	//! The sampling rate to use (for percentage-based sampling)
	double sample_rate;
	//! The seeded phase used for row-count based systematic sampling
	double sample_phase = 0;
	//! Whether the sampling is row-count based or percentage-based
	bool is_percentage = false;
	//! Target number of rows to sample (for row-count based sampling)
	idx_t target_sample_rows = 0;
};

struct TableScanOptions {
	//! Fetch rows one-at-a-time instead of using the regular scans.
	bool force_fetch_row = false;
	//! Target maximum size in bytes for each scan result chunk. 0 = disabled.
	idx_t scan_target_size_bytes = 0;
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
	CollectionScanState table_state;
	//! Transaction-local scan state
	CollectionScanState local_state;
	//! Options for scanning
	TableScanOptions options;
	//! Shared lock over the checkpoint to prevent checkpoints while reading
	shared_ptr<CheckpointLock> checkpoint_lock;
	//! Filter info
	ScanFilterInfo filters;
	//! Sampling info
	ScanSamplingInfo sampling_info;

public:
	//! Takes db so that the scan byte budget (scan_target_size_bytes) can be decided here for every scan entry
	//! point, including those that run without a ClientContext. In-tree scans should always use this overload.
	void Initialize(DatabaseInstance &db, vector<StorageIndex> column_ids,
	                optional_ptr<ClientContext> context = nullptr, optional_ptr<TableFilterSet> table_filters = nullptr,
	                optional_ptr<SampleOptions> table_sampling = nullptr, idx_t estimated_table_row_count = 0);
	//! Only honours a session-local scan_target_size_bytes: without db there is no global default to fall back on.
	//! Kept for out-of-tree callers; in-tree scans should use the overload above.
	void Initialize(vector<StorageIndex> column_ids, optional_ptr<ClientContext> context = nullptr,
	                optional_ptr<TableFilterSet> table_filters = nullptr,
	                optional_ptr<SampleOptions> table_sampling = nullptr, idx_t estimated_table_row_count = 0);

	const vector<StorageIndex> &GetColumnIds();

	ScanFilterInfo &GetFilterInfo();

	ScanSamplingInfo &GetSamplingInfo();

private:
	void InitializeInternal(optional_ptr<DatabaseInstance> db, vector<StorageIndex> column_ids,
	                        optional_ptr<ClientContext> context, optional_ptr<TableFilterSet> table_filters,
	                        optional_ptr<SampleOptions> table_sampling, idx_t estimated_table_row_count);

private:
	//! The column identifiers of the scan
	vector<StorageIndex> column_ids;
};

struct ParallelCollectionScanState {
	ParallelCollectionScanState();
	void AssignRowGroup(optional_ptr<SegmentNode<RowGroup>> row_group);
	optional_ptr<SegmentNode<RowGroup>> GetRootSegment(RowGroupSegmentTree &row_groups) const;
	optional_ptr<SegmentNode<RowGroup>> GetNextRowGroup(RowGroupSegmentTree &row_groups,
	                                                    SegmentNode<RowGroup> &row_group) const;

	//! The row group collection we are scanning
	RowGroupCollection *collection;
	shared_ptr<RowGroupSegmentTree> row_groups;
	optional_ptr<SegmentNode<RowGroup>> current_row_group;
	idx_t vector_index;
	idx_t max_row;
	idx_t batch_index;
	atomic<idx_t> processed_rows;
	optional_idx row_number_base;
	mutex lock;

	//! Optional state for custom row group ordering
	unique_ptr<RowGroupReorderer> reorderer;
	//! Subset of partition indices to scan, if null, scan all
	optional_ptr<const unordered_set<idx_t>> partitions_to_scan;

	//! Whether this row group should be scanned
	bool ShouldScanPartition(SegmentNode<RowGroup> &row_group) const {
		return !partitions_to_scan || partitions_to_scan->count(row_group.GetIndex()) > 0;
	}
};

struct ParallelTableScanState {
	//! Parallel scan state for the table
	ParallelCollectionScanState scan_state;
	//! Parallel scan state for the transaction-local state
	ParallelCollectionScanState local_state;
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
	shared_ptr<RowGroupSegmentTree> row_groups;
	vector<unique_ptr<StorageLockKey>> locks;
	unique_lock<mutex> append_lock;
	SegmentLock segment_lock;
};

} // namespace duckdb
