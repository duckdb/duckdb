#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/common/optional.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/sorting/sort_strategy.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/function/window/window_collection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/main/settings.hpp"
#include "fmt/format.h"

#include <algorithm>
#include <functional>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Util
//===--------------------------------------------------------------------===//
enum class PhysicalCopyToFilePhase : uint8_t { SINK, COMBINE, FINALIZE };

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
// CopyToFileGlobalState hpp
//===--------------------------------------------------------------------===//
class PartitionedCopy;

class CopyToFileGlobalState : public GlobalSinkState {
public:
	CopyToFileGlobalState(const PhysicalCopyToFile &op_p, ClientContext &context_p);
	~CopyToFileGlobalState() override;

public:
	void Initialize();

	void CreateDir(const string &dir_path) DUCKDB_REQUIRES(lock);
	unique_ptr<GlobalFileState> CreateFileState() DUCKDB_REQUIRES(lock);
	optional_ptr<CopyToFileInfo> AddFile(const string &file_name) DUCKDB_REQUIRES(lock);

public:
	const PhysicalCopyToFile &op;
	ClientContext &context;

	//! Lock guarding the global state
	mutable annotated_mutex lock;
	//! Whether the copy was successfully initialized/finalized
	atomic<bool> initialized;
	bool finalized;

	//! The (current) global state
	unique_ptr<GlobalFileState> global_state;
	//! Lambda to create a new global file state
	const std::function<unique_ptr<GlobalFileState>()> create_file_state_fun;

	//! The final batch
	mutable annotated_mutex last_batch_lock;
	unique_ptr<ColumnDataCollection> last_batch DUCKDB_GUARDED_BY(last_batch_lock);

	//! Partitioning state
	unique_ptr<PartitionedCopy> partitioned_copy;

	//! Created directories
	unordered_set<string> created_directories DUCKDB_GUARDED_BY(lock);
	//! The list of files created by this operator
	vector<string> created_files DUCKDB_GUARDED_BY(lock);
	//! Written file info and stats
	vector<unique_ptr<CopyToFileInfo>> written_files DUCKDB_GUARDED_BY(lock);

	atomic<idx_t> rows_copied;
	atomic<idx_t> last_file_offset;
};

//===--------------------------------------------------------------------===//
// CopyToFileLocalState hpp
//===--------------------------------------------------------------------===//
class PartitionedCopyLocalState;

class CopyToFileLocalState : public LocalSinkState {
public:
	CopyToFileLocalState(const PhysicalCopyToFile &op_p, ExecutionContext &context_p, CopyToFileGlobalState &gstate_p,
	                     unique_ptr<LocalFunctionData> local_state);

public:
	const PhysicalCopyToFile &op;
	ExecutionContext &context;
	CopyToFileGlobalState &gstate;

	//! Global/local file state (unpartitioned write)
	unique_ptr<GlobalFileState> global_file_state;
	unique_ptr<LocalFunctionData> local_file_state;

	//! Current append batch (unpartitioned write)
	unique_ptr<ColumnDataCollection> batch;
	ColumnDataAppendState batch_append_state;

	//! Local state (partitioned write)
	unique_ptr<PartitionedCopyLocalState> partitioned_copy_local_state;

	//! Total number of rows copied by this thread
	idx_t total_rows_copied = 0;
};

//===--------------------------------------------------------------------===//
// PartitionedCopy hpp
//===--------------------------------------------------------------------===//
enum class PartitionedCopyStage : uint8_t { SORT, MATERIALIZE, MASK, BATCH, FLUSH, DONE };

struct PartitionedCopyTask {
	PartitionedCopyStage stage = PartitionedCopyStage::DONE;
	idx_t group_idx = 0;
	idx_t thread_idx = 0;
	idx_t begin_idx = 0;
	idx_t end_idx = 0;

	// This differs from PhysicalWindow tasks - need a batch index too
	idx_t batch_idx = 0;
};

struct PartitionedCopyBatch {
	PartitionedCopyBatch(const CopyFunctionBatchAnalyzer batch_analyzer_p,
	                     unique_ptr<PreparedBatchData> prepared_batch_p)
	    : batch_analyzer(batch_analyzer_p), prepared_batch(std::move(prepared_batch_p)) {
	}
	const CopyFunctionBatchAnalyzer batch_analyzer;
	unique_ptr<PreparedBatchData> prepared_batch;
};

struct PartitionWriteInfo {
	unique_ptr<GlobalFileState> file_state;
	idx_t active_writes = 0;
};

struct PartitionedCopyBatchState {
	vector<Value> values;
	optional_ptr<PartitionWriteInfo> write_info;
	vector<unique_ptr<PartitionedCopyBatch>> batches;
};

//! Manages a single partitioned COPY hash bin
class PartitionedCopyHashGroup {
public:
	using ChunkRow = SortStrategy::ChunkRow;

	//! This is the first task that uses a different parallelism model than PhysicalWindow
	static constexpr auto WINDOW_TASKS_DONE = PartitionedCopyStage::BATCH;

public:
	PartitionedCopyHashGroup(PartitionedCopy &partitioned_copy, const ChunkRow &chunk_row, idx_t group_idx);

public:
	PartitionedCopyStage GetStage() const;
	idx_t GetTaskCount() const;
	idx_t InitTasks(idx_t per_thread_p);

	template <typename T>
	static T BinValue(T n, T val) {
		return (n + (val - 1)) / val;
	}

	bool TryPrepareNextStage() DUCKDB_REQUIRES(lock);
	optional<PartitionedCopyTask> TryNextTask() DUCKDB_REQUIRES(lock);
	optional<PartitionedCopyTask> TryNextBatchTask() DUCKDB_REQUIRES(lock);
	optional<PartitionedCopyTask> TryNextFlushTask() DUCKDB_REQUIRES(lock);

	void Sort(ExecutionContext &context, GlobalSinkState &sink, InterruptState &interrupt,
	          const PartitionedCopyTask &task);
	void Materialize(ExecutionContext &context, GlobalSourceState &source, InterruptState &interrupt,
	                 const PartitionedCopyTask &task);
	void Mask(const PartitionedCopyTask &task);
	void Batch(const PartitionedCopyTask &task);
	void Flush(ExecutionContext &execution_context, const PartitionedCopyTask &task);

public:
	//! The PartitionedCopy that this hash group belongs to
	PartitionedCopy &partitioned_copy;

	//! The row count of the group
	const idx_t count;
	//! The number of blocks (chunks) in the group
	const idx_t blocks;
	//! The bin number
	const idx_t group_idx;

	//! The materialized partition data (set after MATERIALIZE stage)
	unique_ptr<ColumnDataCollection> collection;
	//! The partition boundary mask (marks where partition key changes within the bin)
	ValidityMask partition_mask;

	//! Lock for stage transitions and task assignment
	mutable annotated_mutex lock;
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
	//! Used to iterate over chunks (during BATCH stage)
	ColumnDataScanState batch_scan_state DUCKDB_GUARDED_BY(lock);
	//! The batched data (set during BATCH stage), mapping from partition idx to batches (in order)
	vector<unique_ptr<PartitionedCopyBatchState>> batch_states DUCKDB_GUARDED_BY(lock);
	//! Count of batched rows
	atomic<idx_t> batched;

	//! The current partition index for flushing
	idx_t flush_partition_idx DUCKDB_GUARDED_BY(lock) = 0;
	//! Count of flushed partitions
	atomic<idx_t> flushed;
};

//! Manages a partitioned COPY flush
class PartitionedCopyState {
public:
	using PartitionBlock = std::pair<idx_t, idx_t>;

public:
	PartitionedCopyState(PartitionedCopy &partitioned_copy, unique_ptr<GlobalSinkState> global_sink_state);

public:
	void CreateTaskList() DUCKDB_REQUIRES(lock);
	bool HasCompleted() const;
	optional<PartitionedCopyTask> TryAssignTask();
	void ExecuteTask(ExecutionContext &execution_context, const PartitionedCopyTask &task, InterruptState &interrupt);
	void FinishTask(const PartitionedCopyTask &task);

public:
	//! The PartitionedCopy that this state belongs to
	PartitionedCopy &partitioned_copy;

	//! Lock for managing this state
	mutable annotated_mutex lock;

	//! Sink management
	unique_ptr<GlobalSinkState> global_sink_state;
	//! Sort management
	unique_ptr<GlobalSourceState> global_source_state;
	//! Per-hash-bin processing groups (populated by InitHashGroups)
	vector<unique_ptr<PartitionedCopyHashGroup>> hash_groups;
	//! The total number of blocks to process;
	idx_t total_blocks;
	//! The sorted list of (blocks, group_idx) pairs
	vector<PartitionBlock> partition_blocks;
	//! The ordered set of active groups
	vector<idx_t> active_groups DUCKDB_GUARDED_BY(lock);
	//! The number of started tasks
	idx_t next_group DUCKDB_GUARDED_BY(lock);
	//! The number of local states
	idx_t locals DUCKDB_GUARDED_BY(lock);
	//! The number of combined local states
	idx_t combined DUCKDB_GUARDED_BY(lock);
};

//! Manages partitioned COPY
class PartitionedCopy {
public:
	PartitionedCopy(const PhysicalCopyToFile &op, ClientContext &context, CopyToFileGlobalState &copy_gstate);

public:
	//! PhysicalOperator-like interface
	void Sink(ExecutionContext &execution_context, DataChunk &chunk, PartitionedCopyLocalState &lstate,
	          InterruptState &interrupt_state);
	void Combine(ExecutionContext &execution_context, PartitionedCopyLocalState &lstate,
	             InterruptState &interrupt_state);
	void Finalize(Pipeline &pipeline, Event &event, InterruptState &interrupt_state);
	void Flush(ExecutionContext &execution_context, InterruptState &interrupt_state);

	void InitializeFlush() DUCKDB_REQUIRES(lock);

	PartitionWriteInfo &GetPartitionWriteInfo(const vector<Value> &values);
	unique_ptr<GlobalFileState> CreatePartitionFileState(const vector<Value> &values);
	string GetOrCreateDirectory(string path, const vector<Value> &values) DUCKDB_REQUIRES(copy_gstate.lock);

private:
	unique_ptr<const SortStrategy> ConstructSortStrategy() const;
	void CreateNextState();
	bool ShouldStopFlushing() const;

public:
	const PhysicalCopyToFile &op;
	ClientContext &context;
	CopyToFileGlobalState &copy_gstate;

	//! Partition/sort strategy with PhysicalOperator-like interface
	const unique_ptr<const SortStrategy> sort_strategy;

	//! Lock for managing states
	mutable annotated_mutex lock;
	//! Whether a flushing state currently exists
	atomic<bool> flushing;
	//! How many threads are active
	atomic<idx_t> locals;
	//! How many threads did a combine
	atomic<idx_t> combined;
	//! Whether Finalize has been called
	atomic<bool> finalized;

	//! Current sink and combine states
	shared_ptr<PartitionedCopyState> sinking_state DUCKDB_GUARDED_BY(lock);
	shared_ptr<PartitionedCopyState> flushing_state DUCKDB_GUARDED_BY(lock);

	//! The active writes per partition (for partitioned write)
	vector_of_value_map_t<unique_ptr<PartitionWriteInfo>> active_writes DUCKDB_GUARDED_BY(lock);
	vector_of_value_map_t<idx_t> previous_partitions DUCKDB_GUARDED_BY(lock);
	idx_t global_offset DUCKDB_GUARDED_BY(lock) = 0;
};

PartitionedCopyHashGroup::PartitionedCopyHashGroup(PartitionedCopy &partitioned_copy, const ChunkRow &chunk_row,
                                                   idx_t group_idx_p)
    : partitioned_copy(partitioned_copy), count(chunk_row.count), blocks(chunk_row.chunks), group_idx(group_idx_p),
      stage(PartitionedCopyStage::SORT), sorted(0), materialized(0), masked(0), batched(0), flushed(0) {
}

PartitionedCopyStage PartitionedCopyHashGroup::GetStage() const {
	return stage;
}

idx_t PartitionedCopyHashGroup::GetTaskCount() const {
	return group_threads * (static_cast<uint8_t>(WINDOW_TASKS_DONE) - static_cast<uint8_t>(PartitionedCopyStage::SORT));
}

idx_t PartitionedCopyHashGroup::InitTasks(idx_t per_thread_p) {
	per_thread = per_thread_p;
	group_threads = BinValue(blocks, per_thread);
	return GetTaskCount();
}

bool PartitionedCopyHashGroup::TryPrepareNextStage() {
	switch (stage.load()) {
	case PartitionedCopyStage::SORT:
		if (sorted == blocks) {
			stage = PartitionedCopyStage::MATERIALIZE;
			return true;
		}
		return false;
	case PartitionedCopyStage::MATERIALIZE:
		if (materialized == blocks && collection.get()) {
			partition_mask.Initialize(count);
			stage = PartitionedCopyStage::MASK;
			return true;
		}
		return false;
	case PartitionedCopyStage::MASK:
		if (masked == blocks) {
			stage = PartitionedCopyStage::BATCH;
			return true;
		}
		return false;
	case PartitionedCopyStage::BATCH:
		if (batched == count) {
			stage = PartitionedCopyStage::FLUSH;
			return true;
		}
		return false;
	case PartitionedCopyStage::FLUSH:
		if (flushed == batch_states.size()) {
			stage = PartitionedCopyStage::DONE;
			return true;
		}
		return false;
	case PartitionedCopyStage::DONE:
	default:
		return true;
	}
}

optional<PartitionedCopyTask> PartitionedCopyHashGroup::TryNextTask() {
	const auto group_stage = GetStage();
	if (next_task >= GetTaskCount()) {
		// We diverge from the PhysicalWindow parallelism model for these two stages
		switch (group_stage) {
		case PartitionedCopyStage::BATCH:
			return TryNextBatchTask();
		case PartitionedCopyStage::FLUSH:
			return TryNextFlushTask();
		default:
			return std::nullopt;
		}
	}
	const auto task_stage = static_cast<PartitionedCopyStage>(next_task / group_threads);
	if (task_stage != group_stage) {
		return std::nullopt;
	}
	PartitionedCopyTask task;
	task.stage = task_stage;
	task.thread_idx = next_task % group_threads;
	task.group_idx = group_idx;
	task.begin_idx = task.thread_idx * per_thread;
	task.end_idx = MinValue<idx_t>(task.begin_idx + per_thread, blocks);
	++next_task;
	return task;
}

optional<PartitionedCopyTask> PartitionedCopyHashGroup::TryNextBatchTask() {
	if (batch_row_idx == count) {
		return std::nullopt;
	}
	if (partition_mask.RowIsValidUnsafe(batch_row_idx)) {
		batch_states.push_back(make_uniq<PartitionedCopyBatchState>());
	}
	auto &batch_state = *batch_states.back();

	// Reuse these fields
	PartitionedCopyTask task;
	task.stage = PartitionedCopyStage::BATCH;
	task.group_idx = group_idx;
	task.thread_idx = batch_states.size() - 1;
	task.batch_idx = batch_state.batches.size();
	task.begin_idx = batch_row_idx;

	// Find the end_idx
	bool found_next_partition = false;
	idx_t current_batch_size_bytes = 0;
	const auto &segments = collection->GetSegments();
	while (batch_row_idx < count) {
		// Look for the next partition boundary within the current chunk
		// Uses entry-level validity mask iteration to skip 64 rows at a time (see ExecuteFlat in unary_executor.hpp)
		D_ASSERT(batch_row_idx <= batch_scan_state.next_row_index);
		const idx_t chunk_end = batch_scan_state.next_row_index;
		// Skip batch_row_idx itself if it's the start of the current partition
		const idx_t search_start = batch_row_idx + (batch_row_idx == task.begin_idx ? 1 : 0);
		if (search_start < chunk_end) {
			const idx_t begin_entry = search_start / ValidityMask::BITS_PER_VALUE;
			const idx_t end_entry = ValidityMask::EntryCount(chunk_end);
			for (idx_t entry_idx = begin_entry; entry_idx < end_entry; entry_idx++) {
				auto validity_entry = partition_mask.GetValidityEntry(entry_idx);
				if (ValidityMask::NoneValid(validity_entry)) {
					continue; // no partition boundaries in this 64-row block
				}
				const idx_t entry_start = entry_idx * ValidityMask::BITS_PER_VALUE;
				const idx_t row_start = MaxValue(search_start, entry_start);
				const idx_t row_end = MinValue(chunk_end, entry_start + ValidityMask::BITS_PER_VALUE);
				for (idx_t row = row_start; row < row_end; row++) {
					if (ValidityMask::RowIsValid(validity_entry, row - entry_start)) {
						batch_row_idx = row;
						found_next_partition = true;
						break;
					}
				}
				if (found_next_partition) {
					break;
				}
			}
			if (!found_next_partition) {
				batch_row_idx = chunk_end;
			}
		} else {
			batch_row_idx = chunk_end;
		}

		// Did not find the next partition boundary, add chunk
		auto &segment = segments[batch_scan_state.segment_index];
		const auto current_batch_size = batch_row_idx - task.begin_idx;
		current_batch_size_bytes += segment->GetChunkAllocationSize(batch_scan_state.chunk_index - 1);
		const CopyFunctionBatchAnalyzer batch_analyzer(current_batch_size, current_batch_size_bytes,
		                                               partitioned_copy.op.batch_size,
		                                               partitioned_copy.op.batch_size_bytes);

		// Move to the next chunk if we exhausted this one
		if (batch_row_idx == batch_scan_state.next_row_index) {
			idx_t chunk_index;
			idx_t segment_index;
			idx_t row_index;
			collection->NextScanIndex(batch_scan_state, chunk_index, segment_index, row_index);
		}

		if (found_next_partition || batch_analyzer.MeetsFlushCriteria()) {
			break; // Move to the next partition or batch
		}
	}
	task.end_idx = batch_row_idx;

	// Update partition/batch counters
	batch_state.batches.emplace_back();

	return task;
}

optional<PartitionedCopyTask> PartitionedCopyHashGroup::TryNextFlushTask() {
	if (flush_partition_idx == batch_states.size()) {
		return std::nullopt;
	}

	PartitionedCopyTask task;
	task.stage = PartitionedCopyStage::FLUSH;
	task.group_idx = group_idx;
	task.thread_idx = flush_partition_idx++;

	return task;
}

void PartitionedCopyHashGroup::Sort(ExecutionContext &execution_context, GlobalSinkState &sink,
                                    InterruptState &interrupt, const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::SORT);
	OperatorSinkFinalizeInput finalize_input {sink, interrupt};
	partitioned_copy.sort_strategy->SortColumnData(execution_context, group_idx, finalize_input);
	sorted += (task.end_idx - task.begin_idx);
}

void PartitionedCopyHashGroup::Materialize(ExecutionContext &execution_context, GlobalSourceState &source,
                                           InterruptState &interrupt, const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::MATERIALIZE);
	auto unused = make_uniq<LocalSourceState>();
	OperatorSourceInput source_input {source, *unused, interrupt};
	partitioned_copy.sort_strategy->MaterializeColumnData(execution_context, group_idx, source_input);
	materialized += (task.end_idx - task.begin_idx);

	if (materialized >= blocks) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!collection) {
			collection = partitioned_copy.sort_strategy->GetColumnData(group_idx, source_input);
			collection->InitializeScan(batch_scan_state);

			idx_t chunk_index;
			idx_t segment_index;
			idx_t row_index;
			collection->NextScanIndex(batch_scan_state, chunk_index, segment_index, row_index);
		}
	}
}

void PartitionedCopyHashGroup::Mask(const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::MASK);
	D_ASSERT(count > 0);
	D_ASSERT(collection);
	D_ASSERT(partition_mask.IsMaskSet());

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

	masked += (task.end_idx - task.begin_idx);
}

void PartitionedCopyHashGroup::Batch(const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::BATCH);
	const auto &op = partitioned_copy.op;

	// Initialize the scan
	ColumnDataScanState scan_state;
	collection->InitializeScan(scan_state);
	DataChunk scan_chunk;
	collection->InitializeScanChunk(scan_state, scan_chunk);
	collection->Seek(task.begin_idx, scan_state, scan_chunk);
	D_ASSERT(task.begin_idx >= scan_state.current_row_index);

	// Compute which columns to write out and then initialize the write chunk TODO: do this once in PartitionedCopy
	vector<column_t> write_columns;
	vector<LogicalType> write_types;
	unordered_set<idx_t> part_col_set(op.partition_columns.begin(), op.partition_columns.end());
	for (idx_t col_idx = 0; col_idx < op.expected_types.size(); col_idx++) {
		if (op.write_partition_columns || part_col_set.find(col_idx) == part_col_set.end()) {
			write_columns.push_back(col_idx);
			write_types.push_back(collection->Types()[col_idx]);
		}
	}
	DataChunk write_chunk;
	write_chunk.Initialize(partitioned_copy.context, write_types);

	// Initialize the append
	auto batch = make_uniq<ColumnDataCollection>(partitioned_copy.context, write_types);
	ColumnDataAppendState append_state;
	batch->InitializeAppend(append_state);

	vector<Value> values;
	while (scan_state.current_row_index < task.end_idx) {
		// Slice the chunk accordingly
		const auto begin = MaxValue(task.begin_idx, scan_state.current_row_index);
		const auto end = MinValue(scan_state.current_row_index + scan_chunk.size(), task.end_idx);
		scan_chunk.Slice(begin - scan_state.current_row_index, end - begin);

		if (values.empty()) {
			for (const auto &pc : partitioned_copy.op.partition_columns) {
				values.push_back(scan_chunk.GetValue(pc, 0));
			}
		}

		write_chunk.ReferenceColumns(scan_chunk, write_columns);
		batch->Append(append_state, write_chunk);
		collection->Scan(scan_state, scan_chunk);
	}

	// Get pointer to batch state and initialize file if needed (under lock)
	optional_ptr<PartitionedCopyBatchState> batch_state;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		batch_state = batch_states[task.thread_idx];
		D_ASSERT(batch_state->values.empty() || batch_state->values == values);
		if (batch_state->values.empty()) {
			batch_state->values = std::move(values);
			batch_state->write_info = partitioned_copy.GetPartitionWriteInfo(batch_state->values);
		}
	}

	// Prepare the batch (lock-free)
	const CopyFunctionBatchAnalyzer batch_analyzer(*batch, op.batch_size, op.batch_size_bytes);
	auto prepared_batch = op.function.prepare_batch(partitioned_copy.context, *op.bind_data,
	                                                *batch_state->write_info->file_state->data, std::move(batch));
	annotated_lock_guard<annotated_mutex> guard(lock);
	batch_state->batches[task.batch_idx] = make_uniq<PartitionedCopyBatch>(batch_analyzer, std::move(prepared_batch));
	// TODO: if this batch is an entire partition, we could immediately write it without moving it

	batched += (task.end_idx - task.begin_idx);
}

void PartitionedCopyHashGroup::Flush(ExecutionContext &execution_context, const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::FLUSH);

	optional_ptr<PartitionedCopyBatchState> batch_state;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		batch_state = batch_states[task.thread_idx];
	}

	auto &op = partitioned_copy.op;
	auto &file_state = *batch_state->write_info->file_state;
	for (auto &batch : batch_state->batches) {
		file_state.num_batches++;
		DUCKDB_LOG(execution_context.client, PhysicalOperatorLogType, op, "PhysicalCopyToFile", "FlushBatch",
		           {{"file", file_state.path},
		            {"rows", to_string(batch->batch_analyzer.current_batch_size)},
		            {"size", to_string(batch->batch_analyzer.current_batch_size_bytes)},
		            {"reason", EnumUtil::ToString(batch->batch_analyzer.ToReason())}});
		op.function.flush_batch(execution_context.client, *op.bind_data, *file_state.data, *batch->prepared_batch);
	}

	{
		annotated_lock_guard<annotated_mutex> guard(partitioned_copy.lock);
		batch_state->write_info->active_writes--;
	}
}

PartitionedCopyState::PartitionedCopyState(PartitionedCopy &partitioned_copy_p,
                                           unique_ptr<GlobalSinkState> global_sink_state_p)
    : partitioned_copy(partitioned_copy_p), global_sink_state(std::move(global_sink_state_p)), total_blocks(0),
      next_group(0), locals(0), combined(0) {
}

void PartitionedCopyState::CreateTaskList() {
	global_source_state =
	    partitioned_copy.sort_strategy->GetGlobalSourceState(partitioned_copy.context, *global_sink_state);
	const auto &chunk_rows = partitioned_copy.sort_strategy->GetHashGroups(*global_source_state);
	hash_groups.resize(chunk_rows.size());

	for (idx_t group_idx = 0; group_idx < hash_groups.size(); ++group_idx) {
		const auto block_count = chunk_rows[group_idx].chunks;
		if (!block_count) {
			continue;
		}

		auto hash_group = make_uniq<PartitionedCopyHashGroup>(partitioned_copy, chunk_rows[group_idx], group_idx);
		hash_group->batch_base = total_blocks;
		total_blocks += block_count;

		hash_groups[group_idx] = std::move(hash_group);
	}

	if (hash_groups.empty()) {
		return;
	}

	for (idx_t group_idx = 0; group_idx < hash_groups.size(); ++group_idx) {
		auto &hash_group = hash_groups[group_idx];
		if (!hash_group) {
			continue;
		}
		partition_blocks.emplace_back(hash_group->blocks, group_idx);
	}
	std::sort(partition_blocks.begin(), partition_blocks.end(), std::greater<PartitionBlock>());

	auto &ts = TaskScheduler::GetScheduler(partitioned_copy.context);
	const auto &max_block = partition_blocks.front();

	const auto threads = MinValue<idx_t>(locals, NumericCast<idx_t>(ts.NumberOfThreads()));
	const auto aligned_scale = MaxValue<idx_t>(ValidityMask::BITS_PER_VALUE / STANDARD_VECTOR_SIZE, 1);
	const auto aligned_count = PartitionedCopyHashGroup::BinValue(max_block.first, aligned_scale);
	const auto per_thread = aligned_scale * PartitionedCopyHashGroup::BinValue(aligned_count, threads);
	if (!per_thread) {
		throw InternalException("No blocks per thread! %ld threads, %ld groups, %ld blocks, %ld hash group", threads,
		                        partition_blocks.size(), max_block.first, max_block.second);
	}

	for (const auto &b : partition_blocks) {
		hash_groups[b.second]->InitTasks(per_thread);
	}
}

bool PartitionedCopyState::HasCompleted() const {
	annotated_lock_guard<annotated_mutex> global_guard(lock);
	return active_groups.empty() && next_group == partition_blocks.size();
}

optional<PartitionedCopyTask> PartitionedCopyState::TryAssignTask() {
	annotated_lock_guard<annotated_mutex> global_guard(lock);
	for (const auto &group_idx : active_groups) {
		auto &hash_group = hash_groups[group_idx];
		annotated_lock_guard<annotated_mutex> group_guard(hash_group->lock);
		hash_group->TryPrepareNextStage();
		auto task = hash_group->TryNextTask();
		if (!task) {
			continue;
		}
		return task;
	}

	while (next_group < partition_blocks.size()) {
		const auto group_idx = partition_blocks[next_group++].second;
		active_groups.emplace_back(group_idx);

		auto &hash_group = hash_groups[group_idx];
		annotated_lock_guard<annotated_mutex> guard(hash_group->lock);
		hash_group->TryPrepareNextStage();
		auto task = hash_group->TryNextTask();
		if (!task) {
			continue;
		}

		return task;
	}

	return std::nullopt;
}

void PartitionedCopyState::ExecuteTask(ExecutionContext &execution_context, const PartitionedCopyTask &task,
                                       InterruptState &interrupt) {
	auto &hash_group = *hash_groups[task.group_idx];
	switch (task.stage) {
	case PartitionedCopyStage::SORT:
		hash_group.Sort(execution_context, *global_sink_state, interrupt, task);
		break;
	case PartitionedCopyStage::MATERIALIZE:
		hash_group.Materialize(execution_context, *global_source_state, interrupt, task);
		break;
	case PartitionedCopyStage::MASK:
		hash_group.Mask(task);
		break;
	case PartitionedCopyStage::BATCH:
		hash_group.Batch(task);
		break;
	case PartitionedCopyStage::FLUSH:
		hash_group.Flush(execution_context, task);
		break;
	default:
		throw InternalException("Invalid PartitionedCopyStage in PartitionedCopyState::ExecuteTask");
	}
	FinishTask(task);
}

void PartitionedCopyState::FinishTask(const PartitionedCopyTask &task) {
	const auto group_idx = task.group_idx;
	auto &finished_hash_group = hash_groups[group_idx];
	D_ASSERT(finished_hash_group);

	bool hash_group_completed = false;
	if (task.stage == PartitionedCopyStage::FLUSH) {
		annotated_lock_guard<annotated_mutex> group_guard(finished_hash_group->lock);
		hash_group_completed = ++finished_hash_group->flushed == finished_hash_group->batch_states.size();
	}

	if (hash_group_completed) {
		annotated_lock_guard<annotated_mutex> global_guard(lock);
		auto &v = active_groups;
		v.erase(std::remove(v.begin(), v.end(), group_idx), v.end());
		finished_hash_group.reset();
	}
}

class PartitionedCopyLocalState : public LocalSinkState {
public:
	shared_ptr<PartitionedCopyState> current_state;
	unique_ptr<LocalSinkState> sort_strategy_local_state;
	idx_t append_count = 0;
};

PartitionedCopy::PartitionedCopy(const PhysicalCopyToFile &op_p, ClientContext &context_p,
                                 CopyToFileGlobalState &copy_gstate_p)
    : op(op_p), context(context_p), copy_gstate(copy_gstate_p), sort_strategy(ConstructSortStrategy()), flushing(false),
      locals(0), combined(0), finalized(false) {
}

unique_ptr<const SortStrategy> PartitionedCopy::ConstructSortStrategy() const {
	vector<unique_ptr<Expression>> partition_bys;
	for (auto &col : op.partition_columns) {
		partition_bys.push_back(make_uniq<BoundReferenceExpression>(op.expected_types[col], col));
	}
	vector<BoundOrderByNode> order_bys;
	vector<unique_ptr<BaseStatistics>> partition_stats;

	return SortStrategy::Factory(context, partition_bys, order_bys, op.children[0].get().GetTypes(), partition_stats,
	                             op.children[0].get().estimated_cardinality);
}

void PartitionedCopy::CreateNextState() {
	auto global_sink_state = sort_strategy->GetGlobalSinkState(context);
	annotated_lock_guard<annotated_mutex> guard(lock);
	D_ASSERT(!sinking_state);
	sinking_state = make_shared_ptr<PartitionedCopyState>(*this, std::move(global_sink_state));
}

bool PartitionedCopy::ShouldStopFlushing() const {
	return !finalized.load(std::memory_order_relaxed) &&
	       locals.load(std::memory_order_relaxed) == combined.load(std::memory_order_relaxed);
}

void PartitionedCopy::InitializeFlush() {
	if (flushing) {
		return;
	}

	// Move current sink state to combine state, update flushing to true, and create task list
	flushing_state = std::move(sinking_state);
	flushing = true;
}

void PartitionedCopy::Sink(ExecutionContext &execution_context, DataChunk &chunk, PartitionedCopyLocalState &lstate,
                           InterruptState &interrupt_state) {
	// Create new local state if necessary
	if (!lstate.current_state) {
		{
			annotated_lock_guard<annotated_mutex> global_guard(lock);
			if (!sinking_state) {
				auto global_sink_state = sort_strategy->GetGlobalSinkState(context);
				sinking_state = make_shared_ptr<PartitionedCopyState>(*this, std::move(global_sink_state));
			}
			lstate.current_state = sinking_state;
		}
		{
			annotated_lock_guard<annotated_mutex> state_guard(lstate.current_state->lock);
			lstate.current_state->locals++;
		}
		lstate.sort_strategy_local_state = sort_strategy->GetLocalSinkState(execution_context);
		lstate.append_count = 0;
	}

	// Sink into sort strategy
	OperatorSinkInput sort_strategy_sink_input {*lstate.current_state->global_sink_state,
	                                            *lstate.sort_strategy_local_state, interrupt_state};
	sort_strategy->Sink(execution_context, chunk, sort_strategy_sink_input);
	lstate.append_count += chunk.size();

	if (lstate.append_count < Settings::Get<PartitionedWriteFlushThresholdSetting>(context) ||
	    !flushing.load(std::memory_order_relaxed)) {
		return; // Nothing left to do
	}

	bool combine = false;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		combine = flushing_state && RefersToSameObject(*lstate.current_state, *flushing_state);
	}

	if (combine) {
		Combine(execution_context, lstate, interrupt_state);
	}

	Flush(execution_context, interrupt_state);
}

void PartitionedCopy::Combine(ExecutionContext &execution_context, PartitionedCopyLocalState &lstate,
                              InterruptState &interrupt_state) {
	if (!lstate.current_state) {
		return;
	}

	if (!flushing.load(std::memory_order_relaxed)) {
		// Initialize flush if this did not yet happen
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!flushing) {
			InitializeFlush();
		}
	}

	// Combine this thread's local state lock-free
	OperatorSinkCombineInput sort_strategy_combine_input {*lstate.current_state->global_sink_state,
	                                                      *lstate.sort_strategy_local_state, interrupt_state};
	sort_strategy->Combine(execution_context, sort_strategy_combine_input);

	{
		// Finalize if this is the last combine
		annotated_lock_guard<annotated_mutex> guard(lstate.current_state->lock);
		if (++lstate.current_state->combined == lstate.current_state->locals) {
			OperatorSinkFinalizeInput sort_strategy_finalize_input {*lstate.current_state->global_sink_state,
			                                                        interrupt_state};
			sort_strategy->Finalize(context, sort_strategy_finalize_input);
			lstate.current_state->CreateTaskList();
		}
	}

	// Reset local state
	lstate.sort_strategy_local_state.reset();
	lstate.current_state.reset();
}

class PartitionedCopyFinalizeTask : public ExecutorTask {
public:
	PartitionedCopyFinalizeTask(ClientContext &context, shared_ptr<Event> event_p, PartitionedCopy &partitioned_copy_p)
	    : ExecutorTask(context, std::move(event_p), partitioned_copy_p.op), partitioned_copy(partitioned_copy_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		ExecutionContext execution_context(event->GetClientContext(), *thread_context,
		                                   event->Cast<BasePipelineEvent>().pipeline);
		InterruptState interrupt_state(shared_from_this());
		while (partitioned_copy.flushing.load(std::memory_order_relaxed)) {
			event->GetClientContext().InterruptCheck();
			partitioned_copy.Flush(execution_context, interrupt_state);
		}
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "PartitionedCopyFinalizeTask";
	}

private:
	PartitionedCopy &partitioned_copy;
};

class PartitionedCopyFinalizeEvent : public BasePipelineEvent {
public:
	PartitionedCopyFinalizeEvent(Pipeline &pipeline_p, PartitionedCopy &partitioned_copy_p)
	    : BasePipelineEvent(pipeline_p), partitioned_copy(partitioned_copy_p) {
	}

public:
	void Schedule() override {
		annotated_lock_guard<annotated_mutex> global_guard(partitioned_copy.lock);
		if (!partitioned_copy.flushing_state) {
			partitioned_copy.InitializeFlush();
		}
		auto &flushing_state = *partitioned_copy.flushing_state;

		annotated_lock_guard<annotated_mutex> state_guard(flushing_state.lock);
		D_ASSERT(flushing_state.combined == flushing_state.locals);

		vector<shared_ptr<Task>> tasks;
		for (idx_t i = 0; i < flushing_state.locals; i++) {
			tasks.push_back(
			    make_shared_ptr<PartitionedCopyFinalizeTask>(GetClientContext(), shared_from_this(), partitioned_copy));
		}
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		annotated_lock_guard<annotated_mutex> global_guard(partitioned_copy.lock);
		if (partitioned_copy.sinking_state) {
			auto partitioned_copy_finalize_event =
			    make_shared_ptr<PartitionedCopyFinalizeEvent>(*pipeline, partitioned_copy);
			InsertEvent(std::move(partitioned_copy_finalize_event));
		}
	}

private:
	PartitionedCopy &partitioned_copy;
};

void PartitionedCopy::Finalize(Pipeline &pipeline, Event &event, InterruptState &interrupt_state) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	finalized = true;
	if (!sinking_state && !flushing_state) {
		return;
	}

	auto partitioned_copy_finalize_event = make_shared_ptr<PartitionedCopyFinalizeEvent>(pipeline, *this);
	event.InsertEvent(std::move(partitioned_copy_finalize_event));
}

void PartitionedCopy::Flush(ExecutionContext &execution_context, InterruptState &interrupt_state) {
	shared_ptr<PartitionedCopyState> flushing_state_copy;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		flushing_state_copy = flushing_state;
	}

	if (!flushing_state_copy || !flushing_state_copy->global_source_state) {
		return; // Finalization not yet complete, nothing to do
	}

	if (ShouldStopFlushing()) {
		return; // Avoid straggling threads during Combine
	}

	while (auto task = flushing_state_copy->TryAssignTask()) {
		flushing_state_copy->ExecuteTask(execution_context, *task, interrupt_state);
		if (ShouldStopFlushing()) {
			break; // Avoid straggling threads during Combine
		}
	}

	if (!flushing_state_copy->HasCompleted()) {
		return;
	}

	annotated_lock_guard<annotated_mutex> guard(lock);
	if (!flushing || !RefersToSameObject(*flushing_state, *flushing_state_copy)) {
		return;
	}
	flushing_state.reset();
	flushing = false;
	if (finalized && !sinking_state) {
		for (auto &entry : active_writes) {
			auto &file_state = *entry.second->file_state;
			op.function.copy_to_finalize(context, *op.bind_data, *file_state.data);
		}
	}
}

PartitionWriteInfo &PartitionedCopy::GetPartitionWriteInfo(const vector<Value> &values) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		// check if we have already started writing this partition
		auto active_write_entry = active_writes.find(values);
		if (active_write_entry != active_writes.end()) {
			// we have - continue writing in this partition
			active_write_entry->second->active_writes++;
			return *active_write_entry->second;
		}
	}

	auto info = make_uniq<PartitionWriteInfo>();
	info->file_state = CreatePartitionFileState(values);
	auto &result = *info;

	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		info->active_writes = 1;
		// store in active write map
		active_writes.insert(make_pair(values, std::move(info)));
	}
	return result;
}

unique_ptr<GlobalFileState> PartitionedCopy::CreatePartitionFileState(const vector<Value> &values) {
	idx_t offset = 0;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		// check if we need to close any writers before we can continue
		if (active_writes.size() >= Settings::Get<PartitionedWriteMaxOpenFilesSetting>(context)) {
			// we need to! try to close writers
			for (auto &entry : active_writes) {
				if (entry.second->active_writes == 0) {
					// we can evict this entry - evict the partition
					auto &file_state = *entry.second->file_state;
					op.function.copy_to_finalize(context, *op.bind_data, *file_state.data);
					++previous_partitions[entry.first];
					active_writes.erase(entry.first);
					break;
				}
			}
		}

		if (op.hive_file_pattern) {
			auto prev_offset = previous_partitions.find(values);
			if (prev_offset != previous_partitions.end()) {
				offset = prev_offset->second;
			}
		} else {
			offset = global_offset++;
		}
	}

	// Access global state under lock
	annotated_lock_guard<annotated_mutex> guard(copy_gstate.lock);

	// Create a writer for the current file
	auto &fs = FileSystem::GetFileSystem(context);
	const auto hive_path = GetOrCreateDirectory(op.GetTrimmedPath(context, op.file_path), values);
	auto full_path = op.filename_pattern.CreateFilename(fs, hive_path, op.file_extension, offset);
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
	copy_gstate.created_files.push_back(full_path);

	optional_ptr<CopyToFileInfo> written_file_info;
	if (op.return_type != CopyFunctionReturnType::CHANGED_ROWS) {
		written_file_info = copy_gstate.AddFile(full_path);
	}

	// Initialize write
	auto file_state =
	    make_uniq<GlobalFileState>(op.function.copy_to_initialize_global(context, *op.bind_data, full_path), full_path);
	if (op.function.initialize_operator) {
		op.function.initialize_operator(*file_state->data, op);
	}

	if (written_file_info) {
		// Set up the file stats
		op.function.copy_to_get_written_statistics(context, *op.bind_data, *file_state->data,
		                                           *written_file_info->file_stats);

		// Set partition info
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

	return file_state;
}

string PartitionedCopy::GetOrCreateDirectory(string path, const vector<Value> &values) {
	auto &fs = FileSystem::GetFileSystem(context);
	copy_gstate.CreateDir(path);
	if (op.hive_file_pattern) {
		for (idx_t i = 0; i < op.partition_columns.size(); i++) {
			const auto &partition_col_name = op.names[op.partition_columns[i]];
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
			copy_gstate.CreateDir(path);
		}
	}
	return path;
}

//===--------------------------------------------------------------------===//
// CopyToFileGlobalState cpp
//===--------------------------------------------------------------------===//
CopyToFileGlobalState::CopyToFileGlobalState(const PhysicalCopyToFile &op_p, ClientContext &context_p)
    : op(op_p), context(context_p), initialized(false), finalized(false),
      create_file_state_fun([&]() DUCKDB_REQUIRES(lock) { return CreateFileState(); }), rows_copied(0),
      last_file_offset(0) {
}

CopyToFileGlobalState::~CopyToFileGlobalState() {
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

void CopyToFileGlobalState::Initialize() {
	if (initialized) {
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(lock);
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
	auto written_file_info = AddFile(op.file_path);
	if (written_file_info) {
		op.function.copy_to_get_written_statistics(context, *op.bind_data, *global_state->data,
		                                           *written_file_info->file_stats);
	}
	initialized = true;
}

void CopyToFileGlobalState::CreateDir(const string &dir_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (created_directories.find(dir_path) != created_directories.end()) {
		// already attempted to create this directory
		return;
	}
	if (!fs.DirectoryExists(dir_path)) {
		fs.CreateDirectory(dir_path);
	}
	created_directories.insert(dir_path);
}

unique_ptr<GlobalFileState> CopyToFileGlobalState::CreateFileState() {
	auto &fs = FileSystem::GetFileSystem(context);
	string output_path(op.filename_pattern.CreateFilename(fs, op.file_path, op.file_extension, last_file_offset++));
	created_files.push_back(output_path);

	optional_ptr<CopyToFileInfo> written_file_info;
	if (op.return_type != CopyFunctionReturnType::CHANGED_ROWS) {
		written_file_info = AddFile(output_path);
	}

	auto data = op.function.copy_to_initialize_global(context, *op.bind_data, output_path);
	if (written_file_info) {
		op.function.copy_to_get_written_statistics(context, *op.bind_data, *data, *written_file_info->file_stats);
	}

	if (op.function.initialize_operator) {
		op.function.initialize_operator(*data, op);
	}

	return make_uniq<GlobalFileState>(std::move(data), output_path);
}

optional_ptr<CopyToFileInfo> CopyToFileGlobalState::AddFile(const string &file_name) {
	auto file_info = make_uniq<CopyToFileInfo>(file_name);
	optional_ptr<CopyToFileInfo> result;
	if (op.return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS) {
		file_info->file_stats = make_uniq<CopyFunctionFileStatistics>();
		result = file_info.get();
	}
	written_files.push_back(std::move(file_info));
	return result;
}

//===--------------------------------------------------------------------===//
// CopyToFileLocalState hpp
//===--------------------------------------------------------------------===//
CopyToFileLocalState::CopyToFileLocalState(const PhysicalCopyToFile &op_p, ExecutionContext &context_p,
                                           CopyToFileGlobalState &gstate_p, unique_ptr<LocalFunctionData> local_state)
    : op(op_p), context(context_p), gstate(gstate_p), local_file_state(std::move(local_state)) {
	if (op.partition_output) {
		++gstate.partitioned_copy->locals;
		partitioned_copy_local_state = make_uniq<PartitionedCopyLocalState>();
	}
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

		auto state = make_uniq<CopyToFileGlobalState>(*this, context);
		if (!per_thread_output && Rotate() && write_empty_file) {
			annotated_lock_guard<annotated_mutex> guard(state->lock);
			state->global_state = state->CreateFileState();
		}

		if (partition_output) {
			state->partitioned_copy = make_uniq<PartitionedCopy>(*this, context, *state);
		}

		return std::move(state);
	}

	auto state = make_uniq<CopyToFileGlobalState>(*this, context);
	if (write_empty_file) {
		// if we are writing the file also if it is empty - initialize now
		state->Initialize();
	}
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<CopyToFileGlobalState>();
	auto lstate = partition_output ? nullptr : function.copy_to_initialize_local(context, *bind_data);
	return make_uniq<CopyToFileLocalState>(*this, context, gstate, std::move(lstate));
}

SinkResultType PhysicalCopyToFile::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CopyToFileGlobalState>();
	auto &lstate = input.local_state.Cast<CopyToFileLocalState>();

	if (!write_empty_file && !Rotate()) {
		// if we are only writing the file when there are rows to write we need to initialize here
		gstate.Initialize();
	}
	lstate.total_rows_copied += chunk.size();

	if (partition_output) {
		gstate.partitioned_copy->Sink(context, chunk, *lstate.partitioned_copy_local_state, input.interrupt_state);
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
	auto &gstate = input.global_state.Cast<CopyToFileGlobalState>();
	auto &lstate = input.local_state.Cast<CopyToFileLocalState>();
	if (lstate.total_rows_copied == 0) {
		// no rows copied
		return SinkCombineResultType::FINISHED;
	}
	gstate.rows_copied += lstate.total_rows_copied;

	if (partition_output) {
		++gstate.partitioned_copy->combined;
		gstate.partitioned_copy->Combine(context, *lstate.partitioned_copy_local_state, input.interrupt_state);
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
	auto &gstate = input.global_state.Cast<CopyToFileGlobalState>();

	if (partition_output) {
		gstate.partitioned_copy->Finalize(pipeline, event, input.interrupt_state);
		return SinkFinalizeType::READY;
	}

	if (per_thread_output) {
		// already happened in combine
		if (NumericCast<int64_t>(gstate.rows_copied.load()) == 0 && sink_state != nullptr) {
			// no rows from source, write schema to file
			annotated_lock_guard<annotated_mutex> guard(gstate.lock);
			gstate.global_state = gstate.CreateFileState();
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

	gstate.finalized = true;
	return SinkFinalizeType::READY;
}

void PhysicalCopyToFile::FlushBatch(ClientContext &context, GlobalSinkState &gstate_p,
                                    unique_ptr<GlobalFileState> &file_state_ptr,
                                    const std::function<unique_ptr<GlobalFileState>()> &create_file_state_fun,
                                    unique_ptr<LocalFunctionData> &lstate, unique_ptr<ColumnDataCollection> batch,
                                    const PhysicalCopyToFilePhase phase) const {
	auto &gstate = gstate_p.Cast<CopyToFileGlobalState>();

	while (true) {
		// Grab global lock and dereference the current file state (and corresponding lock)
		annotated_unique_lock<annotated_mutex> global_guard(gstate.lock);
		if (!file_state_ptr) {
			file_state_ptr = create_file_state_fun();
		}
		auto &file_state = *file_state_ptr;
		auto &file_lock = file_state_ptr->file_write_lock_if_rotating;
		if (RotateNow(file_state)) {
			// Global state must be rotated. Move to local scope, create an new one, and immediately release global lock
			auto owned_file_state = std::move(file_state_ptr);
			file_state_ptr = create_file_state_fun();
			global_guard.unlock();

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
			global_guard.unlock();

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
	auto &gstate = sink_state->Cast<CopyToFileGlobalState>();
	annotated_lock_guard<annotated_mutex> global_guard(gstate.lock);
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
