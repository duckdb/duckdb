#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/sorting/sort_strategy.hpp"
#include "duckdb/function/window/window_collection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/main/settings.hpp"
#include "fmt/format.h"

#include <algorithm>
#include <functional>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Util
//===--------------------------------------------------------------------===//
enum class PhysicalCopyToFilePhase : uint8_t { SINK, COMBINE, FINALIZE };

struct PartitionWriteInfo {
	unique_ptr<GlobalFileState> global_state;
	idx_t active_writes = 0;
};

struct VectorOfValuesHashFunction {
	uint64_t operator()(const vector<Value> &values) const {
		hash_t result = 0;
		for (auto &val : values) {
			result ^= val.Hash();
		}
		return result;
	}
};

struct VectorOfValuesEquality {
	bool operator()(const vector<Value> &a, const vector<Value> &b) const {
		if (a.size() != b.size()) {
			return false;
		}
		for (idx_t i = 0; i < a.size(); i++) {
			if (ValueOperations::DistinctFrom(a[i], b[i])) {
				return false;
			}
		}
		return true;
	}
};

template <class T>
using vector_of_value_map_t = unordered_map<vector<Value>, T, VectorOfValuesHashFunction, VectorOfValuesEquality>;

void CheckDirectory(FileSystem &fs, const string &file_path, CopyOverwriteMode overwrite_mode) {
	if (overwrite_mode == CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE ||
	    overwrite_mode == CopyOverwriteMode::COPY_APPEND) {
		// with overwrite or ignore we fully ignore the presence of any files instead of erasing them
		return;
	}
	vector<string> file_list;
	vector<string> directory_list;
	directory_list.push_back(file_path);
	for (idx_t dir_idx = 0; dir_idx < directory_list.size(); dir_idx++) {
		auto directory = directory_list[dir_idx];
		fs.ListFiles(directory, [&](const string &path, bool is_directory) {
			auto full_path = fs.JoinPath(directory, path);
			if (is_directory) {
				directory_list.emplace_back(std::move(full_path));
			} else {
				file_list.emplace_back(std::move(full_path));
			}
		});
	}
	if (file_list.empty()) {
		return;
	}
	if (overwrite_mode == CopyOverwriteMode::COPY_OVERWRITE) {
		fs.RemoveFiles(file_list);
	} else {
		throw IOException("Directory \"%s\" is not empty! Enable OVERWRITE option to overwrite files", file_path);
	}
}

struct PhysicalCopyToFileColumnStatsMapData {
	vector<Value> keys;
	vector<Value> values;
};

static PhysicalCopyToFileColumnStatsMapData
CreateColumnStatistics(const case_insensitive_map_t<case_insensitive_map_t<Value>> &column_statistics) {
	PhysicalCopyToFileColumnStatsMapData result;

	//! Use a map to make sure the result has a consistent ordering
	map<string, Value> stats;
	for (auto &entry : column_statistics) {
		map<string, Value> per_column_stats;
		for (auto &stats_entry : entry.second) {
			per_column_stats.emplace(stats_entry.first, stats_entry.second);
		}
		vector<Value> stats_keys;
		vector<Value> stats_values;
		for (auto &stats_entry : per_column_stats) {
			stats_keys.emplace_back(stats_entry.first);
			stats_values.emplace_back(std::move(stats_entry.second));
		}
		auto map_value =
		    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(stats_keys), std::move(stats_values));
		stats.emplace(entry.first, std::move(map_value));
	}
	for (auto &entry : stats) {
		result.keys.emplace_back(entry.first);
		result.values.emplace_back(std::move(entry.second));
	}
	return result;
}

struct GlobalFileState {
	explicit GlobalFileState(unique_ptr<GlobalFunctionData> data_p, const string &path_p)
	    : data(std::move(data_p)), path(path_p), num_batches(0) {
	}
	unique_ptr<GlobalFunctionData> data;
	const string path;
	atomic<idx_t> num_batches;

	//! Lock for more fine-grained locking when rotating files
	StorageLock file_write_lock_if_rotating;
};

//===--------------------------------------------------------------------===//
// PartitionedCopy
//===--------------------------------------------------------------------===//
enum class PartitionedCopyStage : uint8_t { SORT, MATERIALIZE, MASK, BATCH, PREPARE, FLUSH, DONE };

struct PartitionedCopyTask {
	PartitionedCopyStage stage = PartitionedCopyStage::DONE;
	idx_t hash_bin = 0;
	idx_t thread_idx = 0;
	idx_t begin_idx = 0;
	idx_t end_idx = 0;

	unique_ptr<ColumnDataCollection> batch;
};

class PartitionedCopy;

//! Manages a single partitioned COPY hash bin
class PartitionedCopyHashGroup {
public:
	using ChunkRow = SortStrategy::ChunkRow;

public:
	PartitionedCopyHashGroup(const PartitionedCopy &partitioned_copy, const ChunkRow &chunk_row, idx_t hash_bin);

public:
	PartitionedCopyStage GetStage() const;
	idx_t GetTaskCount() const;
	idx_t InitTasks(idx_t per_thread_p);

	template <typename T>
	static T BinValue(T n, T val) {
		return (n + (val - 1)) / val;
	}

	bool TryPrepareNextStage();
	bool TryNextTask(PartitionedCopyTask &task) DUCKDB_REQUIRES(lock);
	bool TryNextBatchTask(PartitionedCopyTask &task) DUCKDB_REQUIRES(lock);
	bool TryNextPrepareTask(PartitionedCopyTask &task) DUCKDB_REQUIRES(lock);
	bool TryNextFlushTask(PartitionedCopyTask &task) DUCKDB_REQUIRES(lock);

	void Sort(ExecutionContext &context, GlobalSinkState &sink, InterruptState &interrupt,
	          const PartitionedCopyTask &task);
	void Materialize(ExecutionContext &context, GlobalSourceState &source, InterruptState &interrupt,
	                 const PartitionedCopyTask &task);
	void Mask(const PartitionedCopyTask &task);
	void Batch(const PartitionedCopyTask &task);
	void Prepare(const PartitionedCopyTask &task);

private:
	void AllocateMask();

public:
	//! The PartitionedCopy that this hash group belongs to
	const PartitionedCopy &partitioned_copy;

	//! The row count of the group
	const idx_t count;
	//! The number of blocks (chunks) in the group
	const idx_t blocks;
	//! The bin number
	const idx_t hash_bin;

	//! The materialized partition data (set after MATERIALIZE stage)
	unique_ptr<ColumnDataCollection> collection;
	//! The partition boundary mask (marks where partition key changes within the bin)
	ValidityMask partition_mask;

	//! Lock for stage transitions and task assignment
	annotated_mutex lock;
	//! The processing stage for this group
	atomic<PartitionedCopyStage> stage;

	//! The next task to assign (monotonic, maps task index → stage)
	idx_t next_task = 0;
	//! The output batch base index
	idx_t batch_base = 0;
	//! The number of blocks per thread (for MASK parallelism)
	idx_t per_thread = 0;
	//! The number of parallel threads for this group
	idx_t group_threads = 0;

	//! Count of sorted blocks
	atomic<idx_t> sorted;
	//! Count of materialized blocks
	atomic<idx_t> materialized;
	//! Count of masked blocks
	atomic<idx_t> masked;

	//! The current row index (during BATCH stage)
	idx_t batch_row_idx DUCKDB_GUARDED_BY(lock) = 0;
	//! The current partition index (hive)
	idx_t batch_partition_idx DUCKDB_GUARDED_BY(lock) = 0;
	//! Used to iterate over chunks (during BATCH stage)
	ColumnDataScanState batch_scan_state DUCKDB_GUARDED_BY(lock);
	//! The batched data (set during BATCH stage), mapping from partition idx to batches (in order)
	unordered_map<idx_t, vector<unique_ptr<ColumnDataCollection>>> batches DUCKDB_GUARDED_BY(lock);
	//! Count of batched rows
	atomic<idx_t> batched;

	//! The prepared batched data (set during PREPARE stage), mapping from partition idx to prepared batches (in order)
	unordered_map<idx_t, vector<unique_ptr<PreparedBatchData>>> prepared_batches DUCKDB_GUARDED_BY(lock);
};

//! Manages a partitioned COPY flush
class PartitionedCopyState {
public:
	PartitionedCopyState(const PartitionedCopy &partitioned_copy, unique_ptr<GlobalSinkState> global_sink_state);

public:
	void InitHashGroups();
	bool TryAssignTask(PartitionedCopyTask &task);
	void ExecuteTask(ExecutionContext &context, const PartitionedCopyTask &task, InterruptState &interrupt);
	bool AllGroupsDone() const;

public:
	//! The PartitionedCopy that this state belongs to
	const PartitionedCopy &partitioned_copy;

	//! Lock for managing this state
	annotated_mutex lock;

	//! Sink management
	unique_ptr<GlobalSinkState> global_sink_state;
	idx_t local_state_count DUCKDB_GUARDED_BY(lock);
	idx_t combine_count DUCKDB_GUARDED_BY(lock);

	//! Sort management
	unique_ptr<GlobalSourceState> global_source_state;
	//! Per-hash-bin processing groups (populated by InitHashGroups)
	vector<unique_ptr<PartitionedCopyHashGroup>> hash_groups;
};

//! Manages partitioned COPY
class PartitionedCopy {
public:
	PartitionedCopy(const PhysicalCopyToFile &op, ClientContext &context);

public:
	void Sink(ExecutionContext &execution_context, DataChunk &chunk, OperatorSinkInput &sink_input);
	void Flush(ExecutionContext &execution_context, GlobalSinkState &gstate, InterruptState &interrupt_state);
	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context);

private:
	unique_ptr<const SortStrategy> ConstructSortStrategy() const;
	void CreateNextState();

public:
	const PhysicalCopyToFile &op;
	ClientContext &context;

	//! Partition/sort strategy with PhysicalOperator-like interface
	const unique_ptr<const SortStrategy> sort_strategy;

	//! Lock for managing states
	annotated_mutex lock;
	//! Whether a flushing state currently exists
	atomic<bool> flushing;

	//! Current sink and combine states
	shared_ptr<PartitionedCopyState> sinking_state;
	shared_ptr<PartitionedCopyState> flushing_state;
};

PartitionedCopyHashGroup::PartitionedCopyHashGroup(const PartitionedCopy &partitioned_copy, const ChunkRow &chunk_row,
                                                   idx_t hash_bin_p)
    : partitioned_copy(partitioned_copy), count(chunk_row.count), blocks(chunk_row.chunks), hash_bin(hash_bin_p),
      stage(PartitionedCopyStage::SORT), sorted(0), materialized(0), masked(0), batched(0) {
}

PartitionedCopyStage PartitionedCopyHashGroup::GetStage() const {
	return stage;
}

idx_t PartitionedCopyHashGroup::GetTaskCount() const {
	return group_threads *
	       (static_cast<uint8_t>(PartitionedCopyStage::DONE) - static_cast<uint8_t>(PartitionedCopyStage::SORT));
}

idx_t PartitionedCopyHashGroup::InitTasks(idx_t per_thread_p) {
	per_thread = per_thread_p;
	group_threads = BinValue(blocks, per_thread);
	return GetTaskCount();
}

bool PartitionedCopyHashGroup::TryPrepareNextStage() {
	annotated_lock_guard<annotated_mutex> prepare_guard(lock);
	switch (stage.load()) {
	case PartitionedCopyStage::SORT:
		if (sorted == blocks) {
			stage = PartitionedCopyStage::MATERIALIZE;
			return true;
		}
		return false;
	case PartitionedCopyStage::MATERIALIZE:
		if (materialized == blocks && collection.get()) {
			stage = PartitionedCopyStage::MASK;
			return true;
		}
		return false;
	case PartitionedCopyStage::MASK:
		if (masked == blocks) {
			stage = PartitionedCopyStage::PREPARE;
			return true;
		}
		return false;
	case PartitionedCopyStage::BATCH:
		if (batched == count) {
			stage = PartitionedCopyStage::PREPARE;
			return true;
		}
		return false;
	case PartitionedCopyStage::PREPARE:
		if (batches.empty()) {
			stage = PartitionedCopyStage::FLUSH;
			return true;
		}
		return false;
	case PartitionedCopyStage::FLUSH:
		if (prepared_batches.empty()) {
			stage = PartitionedCopyStage::DONE;
			return true;
		}
		return false;
	case PartitionedCopyStage::DONE:
	default:
		return true;
	}
}

bool PartitionedCopyHashGroup::TryNextTask(PartitionedCopyTask &task) {
	// We diverge from the PhysicalWindow parallelism model for these two stages
	switch (stage.load()) {
	case PartitionedCopyStage::BATCH:
		return TryNextBatchTask(task);
	case PartitionedCopyStage::PREPARE:
		return TryNextPrepareTask(task);
	case PartitionedCopyStage::FLUSH:
		return TryNextFlushTask(task);
	default:
		break;
	}

	if (next_task >= GetTaskCount()) {
		return false;
	}
	task.stage = static_cast<PartitionedCopyStage>(next_task / group_threads);
	if (task.stage != stage.load()) {
		return false;
	}
	task.thread_idx = next_task % group_threads;
	task.hash_bin = hash_bin;
	task.begin_idx = task.thread_idx * per_thread;
	task.end_idx = MinValue<idx_t>(task.begin_idx + per_thread, blocks);
	++next_task;
	return true;
}

bool PartitionedCopyHashGroup::TryNextBatchTask(PartitionedCopyTask &task) {
	if (batch_row_idx == count) {
		return false;
	}
	auto &partition_batches = batches[batch_partition_idx];

	// Reuse these fields
	task.stage = PartitionedCopyStage::BATCH;
	task.hash_bin = batch_partition_idx;
	task.thread_idx = partition_batches.size();
	task.begin_idx = batch_row_idx;

	// Find the end_idx
	bool found_next_partition = false;
	idx_t current_batch_size_bytes = 0;
	const auto &segments = collection->GetSegments();
	while (batch_row_idx < count) {
		// Look for the next partition boundary within the current chunk
		D_ASSERT(batch_row_idx <= batch_scan_state.next_row_index);
		for (; batch_row_idx < batch_scan_state.next_row_index; batch_row_idx++) {
			if (partition_mask.RowIsValidUnsafe(batch_row_idx)) {
				found_next_partition = true;
				break;
			}
		}

		if (found_next_partition) {
			break; // Found the next partition boundary
		}

		// Did not find the next partition boundary, add chunk
		auto &segment = segments[batch_scan_state.segment_index];
		const auto current_batch_size = batch_row_idx - task.begin_idx;
		current_batch_size_bytes += segment->GetChunkAllocationSize(batch_scan_state.chunk_index);
		const CopyFunctionBatchAnalyzer batch_analyzer(current_batch_size, current_batch_size_bytes,
		                                               partitioned_copy.op.batch_size,
		                                               partitioned_copy.op.batch_size_bytes);
		if (batch_analyzer.MeetsFlushCriteria()) {
			break; // Batch is large enough - stop
		}

		// Move to the next chunk
		idx_t chunk_index;
		idx_t segment_index;
		idx_t row_index;
		const auto success = collection->NextScanIndex(batch_scan_state, chunk_index, segment_index, row_index);
		if (!success) {
			throw InternalException("NextScanIndex failed in PartitionedCopyHashGroup::TryNextBatchTask");
		}
	}
	task.end_idx = batch_row_idx;

	// Update partition/batch counters
	batch_partition_idx += found_next_partition;
	partition_batches.emplace_back();

	return true;
}

bool PartitionedCopyHashGroup::TryNextPrepareTask(PartitionedCopyTask &task) {
	if (batches.empty()) {
		return false;
	}

	task.stage = PartitionedCopyStage::PREPARE;
	const auto it = batches.begin();
	task.hash_bin = it->first;
	for (idx_t partition_batch_idx = 0; partition_batch_idx < it->second.size(); partition_batch_idx++) {
		auto &batch = it->second[partition_batch_idx];
		if (!batch) {
			continue;
		}

		task.thread_idx = partition_batch_idx;
		task.batch = std::move(batch);

		if (partition_batch_idx == it->second.size() - 1) {
			batches.erase(it);
		}
		break;
	}
	return true;
}

bool PartitionedCopyHashGroup::TryNextFlushTask(PartitionedCopyTask &task) {
}

void PartitionedCopyHashGroup::Sort(ExecutionContext &context, GlobalSinkState &sink, InterruptState &interrupt,
                                    const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::SORT);
	OperatorSinkFinalizeInput finalize_input {sink, interrupt};
	partitioned_copy.sort_strategy->SortColumnData(context, hash_bin, finalize_input);
	sorted += (task.end_idx - task.begin_idx);
}

void PartitionedCopyHashGroup::Materialize(ExecutionContext &context, GlobalSourceState &source,
                                           InterruptState &interrupt, const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::MATERIALIZE);
	auto unused = make_uniq<LocalSourceState>();
	OperatorSourceInput source_input {source, *unused, interrupt};
	partitioned_copy.sort_strategy->MaterializeColumnData(context, hash_bin, source_input);
	materialized += (task.end_idx - task.begin_idx);

	if (materialized >= blocks) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!collection) {
			collection = partitioned_copy.sort_strategy->GetColumnData(hash_bin, source_input);
			collection->InitializeScan(batch_scan_state);
		}
	}
}

void PartitionedCopyHashGroup::AllocateMask() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (partition_mask.IsMaskSet()) {
		return;
	}
	partition_mask.Initialize(count);
}

void PartitionedCopyHashGroup::Mask(const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::MASK);
	D_ASSERT(count > 0);
	D_ASSERT(collection);

	AllocateMask();

	const auto begin_entry = partition_mask.EntryCount(task.begin_idx * STANDARD_VECTOR_SIZE);
	const auto end_entry = partition_mask.EntryCount(MinValue<idx_t>(task.end_idx * STANDARD_VECTOR_SIZE, count));

	if (begin_entry >= end_entry) {
		masked += (task.end_idx - task.begin_idx);
		return;
	}

	partition_mask.SetRangeInvalid(count, begin_entry, end_entry);
	if (!task.begin_idx) {
		partition_mask.SetValidUnsafe(0);
	}

	// Only compare partition columns (not order columns)
	const auto key_count = partitioned_copy.op.partition_columns.size();
	auto &scan_cols = partitioned_copy.sort_strategy->sort_ids;

	WindowDeltaScanner(*collection, task.begin_idx, task.end_idx, scan_cols, key_count,
	                   [&](const idx_t row_idx, DataChunk &, DataChunk &, const idx_t ndistinct,
	                       const SelectionVector &distinct, const SelectionVector &) {
		                   for (idx_t i = 0; i < ndistinct; i++) {
			                   partition_mask.SetValidUnsafe(row_idx + distinct.get_index_unsafe(i));
		                   }
	                   });

	masked += (task.begin_idx - task.end_idx);
}

void PartitionedCopyHashGroup::Batch(const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::BATCH);
	// TODO: write partitioned data using partition_mask boundaries
	//  we probably don't want to use begin/end idx here
	//  we need to perform well in case of skew, many tiny partitions, etc.
	//  we also need the written batches to be exactly as specified by batch size (bytes) etc.

	const auto &op = partitioned_copy.op;
	vector<column_t> column_ids;
	unordered_set<idx_t> part_col_set(op.partition_columns.begin(), op.partition_columns.end());
	for (idx_t col_idx = 0; col_idx < op.expected_types.size(); col_idx++) {
		if (op.write_partition_columns || part_col_set.find(col_idx) == part_col_set.end()) {
			column_ids.push_back(col_idx);
		}
	}

	// Initialize the scan
	ColumnDataScanState scan_state;
	collection->InitializeScan(scan_state, column_ids);
	DataChunk scan_chunk;
	collection->InitializeScanChunk(scan_state, scan_chunk);
	collection->Seek(task.begin_idx, scan_state, scan_chunk);
	D_ASSERT(task.begin_idx >= scan_state.current_row_index);

	// Initialize the append
	auto batch = make_uniq<ColumnDataCollection>(partitioned_copy.context, scan_chunk.GetTypes());
	ColumnDataAppendState append_state;
	batch->InitializeAppend(append_state);

	while (scan_state.current_row_index < task.end_idx) {
		// Slice the chunk accordingly
		const auto begin = MaxValue(task.begin_idx, scan_state.current_row_index);
		const auto end = MinValue(scan_state.current_row_index + scan_chunk.size(), task.end_idx);
		scan_chunk.Slice(begin - scan_state.current_row_index, end - begin);

		batch->Append(append_state, scan_chunk);
		collection->Scan(scan_state, scan_chunk);
	}

	// Move the batch into the global state (under lock)
	// TODO: if this batch is an entire partition, we can immediately write it without moving it in here
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		batches[task.hash_bin][task.thread_idx] = std::move(batch);
	}

	batched += (task.end_idx - task.begin_idx);
}

void PartitionedCopyHashGroup::Prepare(const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::PREPARE);
}

PartitionedCopyState::PartitionedCopyState(const PartitionedCopy &partitioned_copy_p,
                                           unique_ptr<GlobalSinkState> global_sink_state_p)
    : partitioned_copy(partitioned_copy_p), global_sink_state(std::move(global_sink_state_p)), local_state_count(0),
      combine_count(0) {
}

void PartitionedCopyState::InitHashGroups() {
	annotated_lock_guard<annotated_mutex> guard(lock);

	D_ASSERT(global_source_state);
	const auto &chunk_rows = partitioned_copy.sort_strategy->GetHashGroups(*global_source_state);

	// Compute per_thread aligned to ValidityMask word boundaries (mirrors WindowGlobalSourceState)
	idx_t max_blocks = 0;
	for (const auto &cr : chunk_rows) {
		max_blocks = MaxValue<idx_t>(max_blocks, cr.chunks);
	}

	const auto threads = local_state_count;
	const auto aligned_scale = MaxValue<idx_t>(ValidityMask::BITS_PER_VALUE / STANDARD_VECTOR_SIZE, 1);
	const auto aligned_count = PartitionedCopyHashGroup::BinValue(max_blocks, aligned_scale);
	const auto per_thread = max_blocks ? aligned_scale * PartitionedCopyHashGroup::BinValue(aligned_count, threads) : 1;

	// Build hash_groups indexed by hash_bin (nullptr for empty bins, like WindowGlobalSourceState)
	hash_groups.resize(chunk_rows.size());
	idx_t total_blocks = 0;
	for (idx_t i = 0; i < chunk_rows.size(); i++) {
		if (!chunk_rows[i].count) {
			continue;
		}
		auto hash_group = make_uniq<PartitionedCopyHashGroup>(partitioned_copy, chunk_rows[i], i);
		hash_group->batch_base = total_blocks;
		total_blocks += chunk_rows[i].chunks;
		hash_group->InitTasks(per_thread);
		hash_groups[i] = std::move(hash_group);
	}
}

bool PartitionedCopyState::TryAssignTask(PartitionedCopyTask &task) {
	for (auto &hash_group : hash_groups) {
		if (!hash_group) {
			continue;
		}
		hash_group->TryPrepareNextStage();
		{
			annotated_lock_guard<annotated_mutex> guard(hash_group->lock);
			if (hash_group->TryNextTask(task)) {
				return true;
			}
		}
	}
	return false;
}

void PartitionedCopyState::ExecuteTask(ExecutionContext &context, const PartitionedCopyTask &task,
                                       InterruptState &interrupt) {
	auto &hash_group = *hash_groups[task.hash_bin];
	switch (task.stage) {
	case PartitionedCopyStage::SORT:
		hash_group.Sort(context, *global_sink_state, interrupt, task);
		break;
	case PartitionedCopyStage::MATERIALIZE:
		hash_group.Materialize(context, *global_source_state, interrupt, task);
		break;
	case PartitionedCopyStage::MASK:
		hash_group.Mask(task);
		break;
	case PartitionedCopyStage::BATCH:
		hash_group.Batch(task);
		break;
	case PartitionedCopyStage::PREPARE:
	default:
		throw InternalException("Invalid PartitionedCopyStage in PartitionedCopyState::ExecuteTask");
	}
}

bool PartitionedCopyState::AllGroupsDone() const {
	for (const auto &hg : hash_groups) {
		if (hg && hg->stage.load() != PartitionedCopyStage::DONE) {
			return false;
		}
	}
	return true;
}

class PartitionedCopyLocalState : public LocalSinkState {
public:
	shared_ptr<PartitionedCopyState> current_state;
	unique_ptr<LocalSinkState> sort_strategy_local_state;
	idx_t append_count = 0;
};

PartitionedCopy::PartitionedCopy(const PhysicalCopyToFile &op_p, ClientContext &context_p)
    : op(op_p), context(context_p), sort_strategy(ConstructSortStrategy()), flushing(false) {
	CreateNextState();
}

unique_ptr<const SortStrategy> PartitionedCopy::ConstructSortStrategy() const {
	vector<unique_ptr<Expression>> partition_bys;
	for (auto &col : op.partition_columns) {
		partition_bys.push_back(make_uniq<BoundReferenceExpression>(op.expected_types[col], col));
	}
	vector<BoundOrderByNode> order_bys;
	vector<unique_ptr<BaseStatistics>> partition_stats;

	return SortStrategy::Factory(context, partition_bys, order_bys, op.children[0].get().GetTypes(), partition_stats,
	                             op.estimated_cardinality);
}

void PartitionedCopy::CreateNextState() {
	auto global_sink_state = sort_strategy->GetGlobalSinkState(context);
	annotated_lock_guard<annotated_mutex> guard(lock);
	D_ASSERT(!sinking_state);
	sinking_state = make_shared_ptr<PartitionedCopyState>(*this, std::move(global_sink_state));
}

unique_ptr<LocalSinkState> PartitionedCopy::GetLocalState(ExecutionContext &) {
	return make_uniq<PartitionedCopyLocalState>();
}

void PartitionedCopy::Sink(ExecutionContext &execution_context, DataChunk &chunk, OperatorSinkInput &sink_input) {
	// Create new local state if necessary
	auto &lstate = sink_input.local_state.Cast<PartitionedCopyLocalState>();
	if (!lstate.current_state) {
		lstate.current_state = sinking_state;
		lstate.sort_strategy_local_state = sort_strategy->GetLocalSinkState(execution_context);
		lstate.append_count = 0;

		annotated_lock_guard<annotated_mutex> guard(lstate.current_state->lock);
		lstate.current_state->local_state_count++;
	}

	// Sink into sort strategy
	OperatorSinkInput sort_strategy_sink_input {*lstate.current_state->global_sink_state,
	                                            *lstate.sort_strategy_local_state, sink_input.interrupt_state};
	sort_strategy->Sink(execution_context, chunk, sort_strategy_sink_input);
	lstate.append_count += chunk.size();

	if (lstate.append_count >= Settings::Get<PartitionedWriteFlushThresholdSetting>(context) &&
	    !flushing.load(std::memory_order_relaxed)) {
		// This thread has exceeded the threshold, but we're not flushing yet, grab lock and check again
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!flushing) {
			D_ASSERT(RefersToSameObject(*lstate.current_state, *sinking_state));
			D_ASSERT(!flushing_state);

			// Move current sink state to combine state, update task to FLUSH, and create a new sink state
			flushing_state = std::move(sinking_state);
			flushing = true;
			CreateNextState();
		}
	}

	if (!flushing.load(std::memory_order_relaxed)) {
		return; // Nothing left to do
	}

	bool combine = false;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		combine = flushing_state && RefersToSameObject(*lstate.current_state, *flushing_state);
	}

	if (combine) {
		// This thread's local state is currently being flushed, combine it (lock-free), and reset
		OperatorSinkCombineInput sort_strategy_combine_input {
		    *lstate.current_state->global_sink_state, *lstate.sort_strategy_local_state, sink_input.interrupt_state};
		sort_strategy->Combine(execution_context, sort_strategy_combine_input);

		// Update state, last combining thread finalizes
		bool finalize;
		{
			annotated_lock_guard<annotated_mutex> guard(lstate.current_state->lock);
			finalize = ++lstate.current_state->combine_count == lstate.current_state->local_state_count;
		}

		if (finalize) {
			OperatorSinkFinalizeInput sort_strategy_finalize_input {*lstate.current_state->global_sink_state,
			                                                        sink_input.interrupt_state};
			sort_strategy->Finalize(context, sort_strategy_finalize_input);
			lstate.current_state->global_source_state =
			    sort_strategy->GetGlobalSourceState(context, *lstate.current_state->global_sink_state);
			lstate.current_state->InitHashGroups();
		}

		lstate.sort_strategy_local_state.reset();
		lstate.current_state.reset();
	}

	Flush(execution_context, sink_input.global_state, sink_input.interrupt_state);
}

void PartitionedCopy::Flush(ExecutionContext &execution_context, GlobalSinkState &gstate,
                            InterruptState &interrupt_state) {
	auto flushing_state_copy = flushing_state;
	if (!flushing_state_copy || !flushing_state_copy->global_source_state) {
		return; // Finalization not yet complete, nothing to do
	}

	PartitionedCopyTask task;
	while (flushing_state_copy->TryAssignTask(task)) {
		flushing_state_copy->ExecuteTask(execution_context, task, interrupt_state);
	}

	if (flushing_state_copy->AllGroupsDone()) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		D_ASSERT(flushing_state && RefersToSameObject(*flushing_state, *flushing_state_copy));
		flushing_state.reset();
		flushing = false;
	}
}

//===--------------------------------------------------------------------===//
// CopyToFunctionGlobalState
//===--------------------------------------------------------------------===//
class CopyToFunctionGlobalState : public GlobalSinkState {
public:
	CopyToFunctionGlobalState(const PhysicalCopyToFile &op_p, ClientContext &context_p);
	~CopyToFunctionGlobalState() override;

public:
	void Initialize();

	string GetOrCreateDirectory(const vector<idx_t> &cols, bool hive_file_pattern, const vector<string> &names,
	                            const vector<Value> &values, string path, FileSystem &fs);
	optional_ptr<CopyToFileInfo> AddFile(const StorageLockKey &l, const string &file_name,
	                                     CopyFunctionReturnType return_type);

	void FinalizePartitions();
	unique_ptr<GlobalFileState> CreatePartitionFileState(const StorageLockKey &l, const vector<Value> &values);
	PartitionWriteInfo &GetPartitionWriteInfo(const vector<Value> &values);
	void FinishPartitionWrite(PartitionWriteInfo &info);

private:
	void CreateDir(const string &dir_path, FileSystem &fs);
	void FinalizePartition(PartitionWriteInfo &info);

public:
	const PhysicalCopyToFile &op;
	ClientContext &context;

	//! Lock guarding the global state
	StorageLock lock;
	//! Whether the copy was successfully initialized/finalized
	atomic<bool> initialized;
	bool finalized;

	//! The (current) global state
	unique_ptr<GlobalFileState> global_state;
	//! Lambda to create a new global file state
	const std::function<unique_ptr<GlobalFileState>(const StorageLockKey &guard)> create_file_state_fun;

	//! The final batch
	annotated_mutex last_batch_lock;
	unique_ptr<ColumnDataCollection> last_batch DUCKDB_GUARDED_BY(last_batch_lock);

	//! Partitioning state
	unique_ptr<PartitionedCopy> partitioned_copy;

	//! Created directories
	unordered_set<string> created_directories;
	//! The list of files created by this operator
	vector<string> created_files;
	//! Written file info and stats
	vector<unique_ptr<CopyToFileInfo>> written_files;

	atomic<idx_t> rows_copied;
	atomic<idx_t> last_file_offset;

private:
	//! The active writes per partition (for partitioned write)
	vector_of_value_map_t<unique_ptr<PartitionWriteInfo>> active_partitioned_writes;
	vector_of_value_map_t<idx_t> previous_partitions;
	idx_t global_offset = 0;
};

CopyToFunctionGlobalState::CopyToFunctionGlobalState(const PhysicalCopyToFile &op_p, ClientContext &context_p)
    : op(op_p), context(context_p), initialized(false), finalized(false),
      create_file_state_fun([&](const StorageLockKey &guard) { return op.CreateFileState(context, *this, guard); }),
      rows_copied(0), last_file_offset(0) {
}

CopyToFunctionGlobalState::~CopyToFunctionGlobalState() {
	if (!initialized || finalized || created_files.empty()) {
		return;
	}
	// If we reach here, the query failed before Finalize was called
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &file : created_files) {
		try {
			fs.TryRemoveFile(file);
		} catch (...) {
			// TryRemoveFile might fail for a variety of reasons, but we can't really propagate error codes here, so
			// best effort cleanup
		}
	}
}

void CopyToFunctionGlobalState::Initialize() {
	if (initialized) {
		return;
	}
	auto write_lock = lock.GetExclusiveLock();
	if (initialized) {
		return;
	}
	// initialize writing to the file
	created_files.push_back(op.file_path);
	global_state = make_uniq<GlobalFileState>(
	    op.function.copy_to_initialize_global(context, *op.bind_data, op.file_path), op.file_path);
	if (op.function.initialize_operator) {
		op.function.initialize_operator(*global_state->data, op);
	}
	auto written_file_info = AddFile(*write_lock, op.file_path, op.return_type);
	if (written_file_info) {
		op.function.copy_to_get_written_statistics(context, *op.bind_data, *global_state->data,
		                                           *written_file_info->file_stats);
	}
	initialized = true;
}

void CopyToFunctionGlobalState::CreateDir(const string &dir_path, FileSystem &fs) {
	if (created_directories.find(dir_path) != created_directories.end()) {
		// already attempted to create this directory
		return;
	}
	if (!fs.DirectoryExists(dir_path)) {
		fs.CreateDirectory(dir_path);
	}
	created_directories.insert(dir_path);
}

string CopyToFunctionGlobalState::GetOrCreateDirectory(const vector<idx_t> &cols, bool hive_file_pattern,
                                                       const vector<string> &names, const vector<Value> &values,
                                                       string path, FileSystem &fs) {
	CreateDir(path, fs);
	if (hive_file_pattern) {
		for (idx_t i = 0; i < cols.size(); i++) {
			const auto &partition_col_name = names[cols[i]];
			const auto &partition_value = values[i];
			string p_dir;
			p_dir += HivePartitioning::Escape(partition_col_name);
			p_dir += "=";
			if (partition_value.IsNull()) {
				p_dir += "__HIVE_DEFAULT_PARTITION__";
			} else {
				p_dir += HivePartitioning::Escape(partition_value.ToString());
			}
			path = fs.JoinPath(path, p_dir);
			CreateDir(path, fs);
		}
	}
	return path;
}

optional_ptr<CopyToFileInfo> CopyToFunctionGlobalState::AddFile(const StorageLockKey &l, const string &file_name,
                                                                CopyFunctionReturnType return_type) {
	D_ASSERT(l.GetType() == StorageLockType::EXCLUSIVE);
	auto file_info = make_uniq<CopyToFileInfo>(file_name);
	optional_ptr<CopyToFileInfo> result;
	if (return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS) {
		file_info->file_stats = make_uniq<CopyFunctionFileStatistics>();
		result = file_info.get();
	}
	written_files.push_back(std::move(file_info));
	return result;
}

void CopyToFunctionGlobalState::FinalizePartition(PartitionWriteInfo &info) {
	if (!info.global_state) {
		// already finalized
		return;
	}
	// finalize the partition
	op.function.copy_to_finalize(context, *op.bind_data, *info.global_state->data);
	info.global_state.reset();
}

void CopyToFunctionGlobalState::FinalizePartitions() {
	// finalize any remaining partitions
	for (auto &entry : active_partitioned_writes) {
		FinalizePartition(*entry.second);
	}
}

unique_ptr<GlobalFileState> CopyToFunctionGlobalState::CreatePartitionFileState(const StorageLockKey &l,
                                                                                const vector<Value> &values) {
	D_ASSERT(l.GetType() == StorageLockType::EXCLUSIVE);
	// check if we need to close any writers before we can continue
	if (active_partitioned_writes.size() >= Settings::Get<PartitionedWriteMaxOpenFilesSetting>(context)) {
		// we need to! try to close writers
		for (auto &entry : active_partitioned_writes) {
			if (entry.second->active_writes == 0) {
				// we can evict this entry - evict the partition
				FinalizePartition(*entry.second);
				++previous_partitions[entry.first];
				active_partitioned_writes.erase(entry.first);
				break;
			}
		}
	}

	idx_t offset = 0;
	if (op.hive_file_pattern) {
		auto prev_offset = previous_partitions.find(values);
		if (prev_offset != previous_partitions.end()) {
			offset = prev_offset->second;
		}
	} else {
		offset = global_offset++;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	// Create a writer for the current file
	auto trimmed_path = op.GetTrimmedPath(context, op.file_path);
	string hive_path =
	    GetOrCreateDirectory(op.partition_columns, op.hive_file_pattern, op.names, values, trimmed_path, fs);
	string full_path(op.filename_pattern.CreateFilename(fs, hive_path, op.file_extension, offset));
	if (op.overwrite_mode == CopyOverwriteMode::COPY_APPEND) {
		// when appending, we first check if the file exists
		while (fs.FileExists(full_path)) {
			// file already exists - re-generate name
			if (!op.filename_pattern.HasUUID()) {
				throw InternalException("CopyOverwriteMode::COPY_APPEND without {uuid} - and file exists");
			}
			full_path = op.filename_pattern.CreateFilename(fs, hive_path, op.file_extension, offset);
		}
	}
	created_files.push_back(full_path);
	optional_ptr<CopyToFileInfo> written_file_info;
	if (op.return_type != CopyFunctionReturnType::CHANGED_ROWS) {
		written_file_info = AddFile(l, full_path, op.return_type);
	}

	// initialize writes
	auto file_state =
	    make_uniq<GlobalFileState>(op.function.copy_to_initialize_global(context, *op.bind_data, full_path), full_path);
	if (written_file_info) {
		// set up the file stats for the copy
		op.function.copy_to_get_written_statistics(context, *op.bind_data, *file_state->data,
		                                           *written_file_info->file_stats);

		// set the partition info
		vector<Value> partition_keys;
		vector<Value> partition_values;
		for (idx_t i = 0; i < op.partition_columns.size(); i++) {
			const auto &partition_col_name = op.names[op.partition_columns[i]];
			const auto &partition_value = values[i];
			partition_keys.emplace_back(partition_col_name);
			partition_values.push_back(partition_value.DefaultCastAs(LogicalType::VARCHAR));
		}
		written_file_info->partition_keys = Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR,
		                                               std::move(partition_keys), std::move(partition_values));
	}
	if (op.function.initialize_operator) {
		op.function.initialize_operator(*file_state->data, op);
	}

	return file_state;
}

PartitionWriteInfo &CopyToFunctionGlobalState::GetPartitionWriteInfo(const vector<Value> &values) {
	auto global_lock = lock.GetExclusiveLock();
	// check if we have already started writing this partition
	auto active_write_entry = active_partitioned_writes.find(values);
	if (active_write_entry != active_partitioned_writes.end()) {
		// we have - continue writing in this partition
		active_write_entry->second->active_writes++;
		return *active_write_entry->second;
	}

	auto info = make_uniq<PartitionWriteInfo>();
	info->global_state = CreatePartitionFileState(*global_lock, values);

	auto &result = *info;
	info->active_writes = 1;
	// store in active write map
	active_partitioned_writes.insert(make_pair(values, std::move(info)));
	return result;
}

void CopyToFunctionGlobalState::FinishPartitionWrite(PartitionWriteInfo &info) {
	auto global_lock = lock.GetExclusiveLock();
	info.active_writes--;
}

//===--------------------------------------------------------------------===//
// CopyToFunctionLocalState
//===--------------------------------------------------------------------===//
class CopyToFunctionLocalState : public LocalSinkState {
public:
	CopyToFunctionLocalState(const PhysicalCopyToFile &op_p, ExecutionContext &context_p,
	                         CopyToFunctionGlobalState &gstate_p, unique_ptr<LocalFunctionData> local_state)
	    : op(op_p), context(context_p), gstate(gstate_p), local_file_state(std::move(local_state)) {
	}

public:
	//! Partitioned append functions
	void AppendToPartition(DataChunk &chunk, InterruptState &interrupt_state);
	void SetDataWithoutPartitions(DataChunk &chunk, const DataChunk &source, const vector<LogicalType> &col_types,
	                              const vector<idx_t> &part_cols);
	void FlushPartitions();

public:
	const PhysicalCopyToFile &op;
	ExecutionContext &context;
	CopyToFunctionGlobalState &gstate;

	//! Global/local file state (unpartitioned write)
	unique_ptr<GlobalFileState> global_file_state;
	unique_ptr<LocalFunctionData> local_file_state;

	//! Current append batch (unpartitioned write)
	unique_ptr<ColumnDataCollection> batch;
	ColumnDataAppendState batch_append_state;

	//! Local state (partitioned write)
	unique_ptr<LocalSinkState> partitioned_copy_local_state;

	//! Total number of rows copied by this thread
	idx_t total_rows_copied = 0;
};

void CopyToFunctionLocalState::InitializePartitionedAppendState() {
	partitioned_copy_local_state = gstate.hashed_sort->GetLocalSinkState(context);
}

void CopyToFunctionLocalState::AppendToPartition(DataChunk &chunk, InterruptState &interrupt_state) {
	if (!partitioned_copy_local_state) {
		// re-initialize the append
		InitializePartitionedAppendState();
	}

	// TODO: maybe check sink result type?
	OperatorSinkInput input {*gstate.hashed_sort_global_sink, *partitioned_copy_local_state, interrupt_state};
	gstate.hashed_sort->Sink(context, chunk, input);

	append_count += chunk.size();
	if (append_count >= Settings::Get<PartitionedWriteFlushThresholdSetting>(context.client)) {
		// flush all cached partitions
		FlushPartitions();
	}
}

void CopyToFunctionLocalState::ResetPartitionedAppendState() {
	partitioned_copy_local_state.reset();
	append_count = 0;
}

void CopyToFunctionLocalState::SetDataWithoutPartitions(DataChunk &chunk, const DataChunk &source,
                                                        const vector<LogicalType> &col_types,
                                                        const vector<idx_t> &part_cols) {
	D_ASSERT(source.ColumnCount() == col_types.size());
	auto types = LogicalCopyToFile::GetTypesWithoutPartitions(col_types, part_cols, false);
	chunk.InitializeEmpty(types);
	set<idx_t> part_col_set(part_cols.begin(), part_cols.end());
	idx_t new_col_id = 0;
	for (idx_t col_idx = 0; col_idx < source.ColumnCount(); col_idx++) {
		if (part_col_set.find(col_idx) == part_col_set.end()) {
			chunk.data[new_col_id].Reference(source.data[col_idx]);
			new_col_id++;
		}
	}
	chunk.SetCardinality(source.size());
}

void CopyToFunctionLocalState::FlushPartitions() {
	if (!partitioned_copy_local_state) {
		return;
	}
	part_buffer->FlushAppendState(*part_buffer_append_state);
	auto &partitions = part_buffer->GetPartitions();
	auto partition_key_map = part_buffer->GetReverseMap();

	const auto types_without_partition_cols =
	    LogicalCopyToFile::GetTypesWithoutPartitions(op.expected_types, op.partition_columns, false);

	for (idx_t i = 0; i < partitions.size(); i++) {
		auto entry = partition_key_map.find(i);
		if (entry == partition_key_map.end()) {
			continue;
		}
		// get the partition write info for this buffer
		auto &info = gstate.GetPartitionWriteInfo(entry->second->values);

		const auto create_file_state_fun = [&](const StorageLockKey &guard) {
			return gstate.CreatePartitionFileState(guard, entry->second->values);
		};

		auto local_copy_state = op.function.copy_to_initialize_local(context, *op.bind_data);

		unique_ptr<ColumnDataCollection> partition_batch;
		ColumnDataAppendState partition_batch_append_state;

		// push the chunks into the write state
		for (auto &chunk : partitions[i]->Chunks()) {
			if (!partition_batch.get()) {
				partition_batch = make_uniq<ColumnDataCollection>(
				    context.client, op.write_partition_columns ? op.expected_types : types_without_partition_cols);
				partition_batch->InitializeAppend(partition_batch_append_state);
			}

			if (op.write_partition_columns) {
				partition_batch->Append(partition_batch_append_state, chunk);
			} else {
				DataChunk filtered_chunk;
				SetDataWithoutPartitions(filtered_chunk, chunk, op.expected_types, op.partition_columns);
				partition_batch->Append(partition_batch_append_state, filtered_chunk);
			}

			const CopyFunctionBatchAnalyzer batch_analyzer(*partition_batch, op.batch_size, op.batch_size_bytes);
			if (batch_analyzer.MeetsFlushCriteria()) {
				partition_batch_append_state.current_chunk_state.handles.clear();
				op.FlushBatch(context.client, gstate, info.global_state, create_file_state_fun, local_copy_state,
				              std::move(partition_batch), PhysicalCopyToFilePhase::COMBINE);
				partition_batch = nullptr;
			}
		}

		if (partition_batch) {
			partition_batch_append_state.current_chunk_state.handles.clear();
			op.FlushBatch(context.client, gstate, info.global_state, create_file_state_fun, local_copy_state,
			              std::move(partition_batch), PhysicalCopyToFilePhase::COMBINE);
			partition_batch = nullptr;
		}

		local_copy_state.reset();
		partitions[i].reset();
		gstate.FinishPartitionWrite(info);
	}
	ResetPartitionedAppendState();
}

//===--------------------------------------------------------------------===//
// PhysicalCopyToFile
//===--------------------------------------------------------------------===//
PhysicalCopyToFile::PhysicalCopyToFile(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function_p,
                                       unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::COPY_TO_FILE, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data)), parallel(false) {
}

InsertionOrderPreservingMap<string> PhysicalCopyToFile::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["FORMAT"] = StringUtil::Upper(function.name);
	return result;
}

string PhysicalCopyToFile::GetTrimmedPath(ClientContext &context, const string &file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	string trimmed_path = file_path;
	StringUtil::RTrim(trimmed_path, fs.PathSeparator(trimmed_path));
	return trimmed_path;
}

void PhysicalCopyToFile::MoveTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto file_path = GetNonTmpFile(context, tmp_file_path);
	fs.MoveFile(tmp_file_path, file_path);
}

string PhysicalCopyToFile::GetNonTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);

	auto path = StringUtil::GetFilePath(tmp_file_path);
	auto base = StringUtil::GetFileName(tmp_file_path);

	auto prefix = base.find("tmp_");
	if (prefix == 0) {
		base = base.substr(4);
	}

	return fs.JoinPath(path, base);
}

void PhysicalCopyToFile::ReturnStatistics(DataChunk &chunk, idx_t row_idx, CopyToFileInfo &info) {
	auto &file_stats = *info.file_stats;

	// filename VARCHAR
	chunk.SetValue(0, row_idx, info.file_path);
	// count BIGINT
	chunk.SetValue(1, row_idx, Value::UBIGINT(file_stats.row_count));
	// file size bytes BIGINT
	chunk.SetValue(2, row_idx, Value::UBIGINT(file_stats.file_size_bytes));
	// footer size bytes BIGINT
	chunk.SetValue(3, row_idx, file_stats.footer_size_bytes);
	// column statistics map(varchar, map(varchar, varchar))
	auto column_stats = CreateColumnStatistics(file_stats.column_statistics);
	auto map_val_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	chunk.SetValue(
	    4, row_idx,
	    Value::MAP(LogicalType::VARCHAR, map_val_type, std::move(column_stats.keys), std::move(column_stats.values)));

	// partition_keys map(varchar, varchar)
	chunk.SetValue(5, row_idx, info.partition_keys);
}

unique_ptr<GlobalFileState> PhysicalCopyToFile::CreateFileState(ClientContext &context, GlobalSinkState &sink,
                                                                const StorageLockKey &global_lock) const {
	auto &gstate = sink.Cast<CopyToFunctionGlobalState>();
	idx_t this_file_offset = gstate.last_file_offset++;
	auto &fs = FileSystem::GetFileSystem(context);
	string output_path(filename_pattern.CreateFilename(fs, file_path, file_extension, this_file_offset));
	gstate.created_files.push_back(output_path);
	optional_ptr<CopyToFileInfo> written_file_info;
	if (return_type != CopyFunctionReturnType::CHANGED_ROWS) {
		written_file_info = gstate.AddFile(global_lock, output_path, return_type);
	}
	auto data = function.copy_to_initialize_global(context, *bind_data, output_path);
	if (written_file_info) {
		function.copy_to_get_written_statistics(context, *bind_data, *data, *written_file_info->file_stats);
	}
	if (function.initialize_operator) {
		function.initialize_operator(*data, *this);
	}
	return make_uniq<GlobalFileState>(std::move(data), output_path);
}

bool PhysicalCopyToFile::Rotate() const {
	return file_size_bytes.IsValid() || batches_per_file.IsValid();
}

bool PhysicalCopyToFile::RotateNow(GlobalFileState &global_state) const {
	if (file_size_bytes.IsValid()) {
		return function.file_size_bytes(*global_state.data) >= file_size_bytes.GetIndex();
	}
	if (batches_per_file.IsValid()) {
		return global_state.num_batches >= batches_per_file.GetIndex();
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Sink Interface
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PhysicalCopyToFile::GetGlobalSinkState(ClientContext &context) const {
	if (partition_output || per_thread_output || Rotate()) {
		auto &fs = FileSystem::GetFileSystem(context);
		if (!fs.IsRemoteFile(file_path)) {
			if (fs.FileExists(file_path)) {
				// the target file exists AND is a file (not a directory)
				// for local files we can remove the file if OVERWRITE_OR_IGNORE is enabled
				if (overwrite_mode == CopyOverwriteMode::COPY_OVERWRITE) {
					fs.RemoveFile(file_path);
				} else {
					throw IOException("Cannot write to \"%s\" - it exists and is a file, not a directory! Enable "
					                  "OVERWRITE option to overwrite the file",
					                  file_path);
				}
			}
		}

		// what if the target exists and is a directory
		if (!fs.DirectoryExists(file_path)) {
			fs.CreateDirectory(file_path);
		} else {
			CheckDirectory(fs, file_path, overwrite_mode);
		}

		auto state = make_uniq<CopyToFunctionGlobalState>(*this, context);
		if (!per_thread_output && Rotate() && write_empty_file) {
			auto global_lock = state->lock.GetExclusiveLock();
			state->global_state = CreateFileState(context, *state, *global_lock);
		}

		if (partition_output) {
			state->partitioned_copy = make_uniq<PartitionedCopy>(*this, context);
		}

		return std::move(state);
	}

	auto state = make_uniq<CopyToFunctionGlobalState>(*this, context);
	if (write_empty_file) {
		// if we are writing the file also if it is empty - initialize now
		state->Initialize();
	}
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<CopyToFunctionGlobalState>();
	if (partition_output) {
		auto lstate = make_uniq<CopyToFunctionLocalState>(*this, context, gstate, nullptr);
		lstate->InitializePartitionedAppendState();
		return std::move(lstate);
	}
	auto res = make_uniq<CopyToFunctionLocalState>(*this, context, gstate,
	                                               function.copy_to_initialize_local(context, *bind_data));
	return std::move(res);
}

SinkResultType PhysicalCopyToFile::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &lstate = input.local_state.Cast<CopyToFunctionLocalState>();

	if (!write_empty_file && !Rotate()) {
		// if we are only writing the file when there are rows to write we need to initialize here
		gstate.Initialize();
	}
	lstate.total_rows_copied += chunk.size();

	if (partition_output) {
		lstate.AppendToPartition(chunk, input.interrupt_state);
		return SinkResultType::NEED_MORE_INPUT;
	}

	if (!lstate.batch) {
		lstate.batch = make_uniq<ColumnDataCollection>(context.client, expected_types);
		lstate.batch->InitializeAppend(lstate.batch_append_state);
	}
	lstate.batch->Append(lstate.batch_append_state, chunk);

	const CopyFunctionBatchAnalyzer batch_analyzer(*lstate.batch, batch_size, batch_size_bytes);
	if (batch_analyzer.MeetsFlushCriteria()) {
		lstate.batch_append_state.current_chunk_state.handles.clear();
		auto &file_state_ptr = per_thread_output ? lstate.global_file_state : gstate.global_state;
		FlushBatch(context.client, gstate, file_state_ptr, gstate.create_file_state_fun, lstate.local_file_state,
		           std::move(lstate.batch), PhysicalCopyToFilePhase::SINK);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCopyToFile::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &lstate = input.local_state.Cast<CopyToFunctionLocalState>();
	if (lstate.total_rows_copied == 0) {
		// no rows copied
		return SinkCombineResultType::FINISHED;
	}
	gstate.rows_copied += lstate.total_rows_copied;

	if (partition_output) {
		// flush all remaining partitions
		lstate.FlushPartitions();
		return SinkCombineResultType::FINISHED;
	}

	if (per_thread_output) {
		if (lstate.batch) {
			FlushBatch(context.client, gstate, lstate.global_file_state, gstate.create_file_state_fun,
			           lstate.local_file_state, std::move(lstate.batch), PhysicalCopyToFilePhase::COMBINE);
		}
		function.copy_to_finalize(context.client, *bind_data, *lstate.global_file_state->data);
		return SinkCombineResultType::FINISHED;
	}

	if (!lstate.batch) {
		return SinkCombineResultType::FINISHED;
	}

	unique_ptr<ColumnDataCollection> batch;
	{
		annotated_lock_guard<annotated_mutex> guard(gstate.last_batch_lock);
		if (gstate.last_batch) {
			const auto count = gstate.last_batch->Count() + lstate.batch->Count();
			const auto size_in_bytes = gstate.last_batch->SizeInBytes() + lstate.batch->SizeInBytes();
			const CopyFunctionBatchAnalyzer batch_analyzer(count, size_in_bytes, batch_size, batch_size_bytes);
			if (batch_analyzer.MeetsFlushCriteria()) {
				// Combining makes us overshoot, make sure the smallest one gets flushed now
				auto &small = lstate.batch->Count() < gstate.last_batch->Count() ? lstate.batch : gstate.last_batch;
				auto &large = lstate.batch->Count() < gstate.last_batch->Count() ? gstate.last_batch : lstate.batch;
				batch = std::move(large);
				gstate.last_batch = std::move(small);
			} else {
				gstate.last_batch->Combine(*lstate.batch);
			}
		} else {
			gstate.last_batch = std::move(lstate.batch);
		}
	}

	if (!batch) {
		return SinkCombineResultType::FINISHED;
	}

	auto &file_state_ptr = per_thread_output ? lstate.global_file_state : gstate.global_state;
	FlushBatch(context.client, gstate, file_state_ptr, gstate.create_file_state_fun, lstate.local_file_state,
	           std::move(batch), PhysicalCopyToFilePhase::COMBINE);

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto result = FinalizeInternal(context, input.global_state);
	gstate.finalized = true;
	return result;
}

SinkFinalizeType PhysicalCopyToFile::FinalizeInternal(ClientContext &context, GlobalSinkState &global_state) const {
	auto &gstate = global_state.Cast<CopyToFunctionGlobalState>();

	if (partition_output) {
		// finalize any outstanding partitions
		gstate.FinalizePartitions();
		return SinkFinalizeType::READY;
	}

	if (per_thread_output) {
		// already happened in combine
		if (NumericCast<int64_t>(gstate.rows_copied.load()) == 0 && sink_state != nullptr) {
			// no rows from source, write schema to file
			auto global_lock = gstate.lock.GetExclusiveLock();
			gstate.global_state = CreateFileState(context, *sink_state, *global_lock);
			function.copy_to_finalize(context, *bind_data, *gstate.global_state->data);
		}
		return SinkFinalizeType::READY;
	}

	annotated_lock_guard<annotated_mutex> guard(gstate.last_batch_lock);
	if (gstate.last_batch) {
		unique_ptr<LocalFunctionData> lstate;
		FlushBatch(context, gstate, gstate.global_state, gstate.create_file_state_fun, lstate,
		           std::move(gstate.last_batch), PhysicalCopyToFilePhase::FINALIZE);
	}

	if (function.copy_to_finalize && gstate.global_state) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state->data);

		if (use_tmp_file) {
			D_ASSERT(!per_thread_output);
			D_ASSERT(!partition_output);
			D_ASSERT(!file_size_bytes.IsValid());
			D_ASSERT(!Rotate());
			MoveTmpFile(context, file_path);
		}
	}

	return SinkFinalizeType::READY;
}

void PhysicalCopyToFile::FlushBatch(
    ClientContext &context, GlobalSinkState &gstate_p, unique_ptr<GlobalFileState> &file_state_ptr,
    const std::function<unique_ptr<GlobalFileState>(const StorageLockKey &guard)> &create_file_state_fun,
    unique_ptr<LocalFunctionData> &lstate, unique_ptr<ColumnDataCollection> batch,
    const PhysicalCopyToFilePhase phase) const {
	auto &gstate = gstate_p.Cast<CopyToFunctionGlobalState>();

	while (true) {
		// Grab global lock and dereference the current file state (and corresponding lock)
		auto global_guard = gstate.lock.GetExclusiveLock();
		if (!file_state_ptr) {
			file_state_ptr = create_file_state_fun(*global_guard);
		}
		auto &file_state = *file_state_ptr;
		auto &file_lock = file_state_ptr->file_write_lock_if_rotating;
		if (RotateNow(file_state)) {
			// Global state must be rotated. Move to local scope, create an new one, and immediately release global lock
			auto owned_file_state = std::move(file_state_ptr);
			file_state_ptr = create_file_state_fun(*global_guard);
			global_guard.reset();

			// This thread now waits for the exclusive lock on this file while other threads complete their writes
			// Note that new writes can still start, as there is already a new global state
			auto file_guard = file_lock.GetExclusiveLock();
			function.copy_to_finalize(context, *bind_data, *owned_file_state->data);
		} else {
			// We are going to flush a batch to this file, increment counter as soon as possible
			file_state.num_batches++;

			// Get shared file write lock while holding global lock,
			// so file can't be rotated before we get the write lock
			auto file_guard = file_lock.GetSharedLock();

			// Because we got the shared lock on the file, we're sure that it will keep existing until we release it
			global_guard.reset();

			const CopyFunctionBatchAnalyzer batch_analyzer(*batch, batch_size, batch_size_bytes);
			DUCKDB_LOG(context, PhysicalOperatorLogType, *this, "PhysicalCopyToFile", "FlushBatch",
			           {{"file", file_state.path},
			            {"rows", to_string(batch->Count())},
			            {"size", to_string(batch->SizeInBytes())},
			            {"reason", EnumUtil::ToString(batch_analyzer.ToReason())}});

			if (function.prepare_batch && function.flush_batch) {
				auto prepared_batch = function.prepare_batch(context, *bind_data, *file_state.data, std::move(batch));
				function.flush_batch(context, *bind_data, *file_state.data, *prepared_batch);
			} else {
				// Old API - make dummy stuff and manually prepare/flush batch
				ThreadContext thread_context(context);
				ExecutionContext execution_context(context, thread_context, nullptr);
				if (!lstate) {
					lstate = function.copy_to_initialize_local(execution_context, *bind_data);
				}
				for (auto &chunk : batch->Chunks()) {
					function.copy_to_sink(execution_context, *bind_data, *file_state.data, *lstate, chunk);
				}
				if (phase != PhysicalCopyToFilePhase::SINK) {
					function.copy_to_combine(execution_context, *bind_data, *file_state.data, *lstate);
				}
			}

			break;
		}
	}
}

//===--------------------------------------------------------------------===//
// Source Interface
//===--------------------------------------------------------------------===//
class CopyToFileGlobalSourceState : public GlobalSourceState {
public:
	CopyToFileGlobalSourceState() {
	}

	idx_t offset = 0;

	idx_t MaxThreads() override {
		return 1;
	}
};

unique_ptr<GlobalSourceState> PhysicalCopyToFile::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CopyToFileGlobalSourceState>();
}

SourceResultType PhysicalCopyToFile::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<CopyToFunctionGlobalState>();
	if (return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS) {
		auto &source_state = input.global_state.Cast<CopyToFileGlobalSourceState>();
		idx_t next_end = MinValue<idx_t>(source_state.offset + STANDARD_VECTOR_SIZE, gstate.written_files.size());
		idx_t count = next_end - source_state.offset;
		for (idx_t i = 0; i < count; i++) {
			auto &file_entry = *gstate.written_files[source_state.offset + i];
			if (use_tmp_file) {
				file_entry.file_path = GetNonTmpFile(context.client, file_entry.file_path);
			}
			ReturnStatistics(chunk, i, file_entry);
		}
		chunk.SetCardinality(count);
		source_state.offset += count;
		return source_state.offset < gstate.written_files.size() ? SourceResultType::HAVE_MORE_OUTPUT
		                                                         : SourceResultType::FINISHED;
	}

	chunk.SetCardinality(1);
	switch (return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_copied.load())));
		break;
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST: {
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_copied.load())));
		vector<Value> file_name_list;
		for (auto &file_info : gstate.written_files) {
			if (use_tmp_file) {
				file_name_list.emplace_back(GetNonTmpFile(context.client, file_info->file_path));
			} else {
				file_name_list.emplace_back(file_info->file_path);
			}
		}
		chunk.SetValue(1, 0, Value::LIST(LogicalType::VARCHAR, std::move(file_name_list)));
		break;
	}
	default:
		throw NotImplementedException("Unknown CopyFunctionReturnType");
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
