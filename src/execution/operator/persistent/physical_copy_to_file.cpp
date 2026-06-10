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

struct VectorOfValuesLess {
	bool operator()(const vector<Value> &a, const vector<Value> &b) const {
		const auto count = MinValue(a.size(), b.size());
		for (idx_t i = 0; i < count; i++) {
			if (ValueOperations::DistinctLessThan(a[i], b[i])) {
				return true;
			}
			if (ValueOperations::DistinctLessThan(b[i], a[i])) {
				return false;
			}
		}
		return a.size() < b.size();
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
	annotated_mutex lock;
	unique_ptr<GlobalFunctionData> data;
	const string path;
	idx_t num_batches DUCKDB_GUARDED_BY(lock);
};

static bool PhysicalCopyRotateNow(const PhysicalCopyToFile &op, GlobalFileState &global_state)
    DUCKDB_REQUIRES(global_state.lock);

//===--------------------------------------------------------------------===//
// Copy State Declarations
//===--------------------------------------------------------------------===//
class PartitionedCopy;

class CopyToFileGlobalState : public GlobalSinkState {
public:
	CopyToFileGlobalState(const PhysicalCopyToFile &op_p, ClientContext &context_p);
	~CopyToFileGlobalState() override;

public:
	void Initialize();

	void CreateDir(const string &dir_path) DUCKDB_REQUIRES(lock);
	unique_ptr<GlobalFileState> CreateFileStateLocked(string output_path = string(),
	                                                  optional_ptr<const vector<Value>> partition_values = nullptr)
	    DUCKDB_REQUIRES(lock);
	unique_ptr<GlobalFileState> CreateFileState(string output_path = string(),
	                                            optional_ptr<const vector<Value>> partition_values = nullptr)
	    DUCKDB_EXCLUDES(lock);
	unique_ptr<GlobalFileState> FinalizeFileStateLocked(unique_ptr<GlobalFileState> file_state) DUCKDB_REQUIRES(lock);
	void FinalizeFileState(unique_ptr<GlobalFileState> file_state) DUCKDB_EXCLUDES(lock);

	unique_ptr<GlobalFileState> TryFinalizeOwnedFileStateLocked() DUCKDB_REQUIRES(lock);
	void TryFinalizeOwnedFileState() DUCKDB_EXCLUDES(lock);

private:
	optional_ptr<CopyToFileInfo> AddFile(const string &file_name) DUCKDB_REQUIRES(lock);

public:
	const PhysicalCopyToFile &op;
	ClientContext &context;

	//! Lock guarding the global state
	mutable annotated_mutex lock;
	//! Whether the copy was successfully initialized/finalized
	atomic<bool> initialized;
	atomic<bool> finalized;

	//! We write to files using the Prepare/Flush batch API:
	//! - Prepare gets the data ready and can take a lot of time
	//! - Flush appends to the file, which increments FILE_SIZE_BYTES/BATCHES_PER_FILE
	//! Therefore, we must delay deciding which file to flush; otherwise, parallel writes overshoot
	//! All Prepare are done against this state
	atomic<optional_ptr<GlobalFileState>> prepare_global_state;
	unique_ptr<GlobalFileState> prepare_global_state_owned DUCKDB_GUARDED_BY(lock);

	//! The (current) global state
	unique_ptr<GlobalFileState> global_state;
	//! Lambda to create a new global file state
	const std::function<unique_ptr<GlobalFileState>()> create_file_state_fun;
	unordered_set<unique_ptr<GlobalFileState> *> creating_file_states DUCKDB_GUARDED_BY(lock);

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

	//! Counters
	atomic<idx_t> rows_copied;
	atomic<idx_t> last_file_offset;
};

//===--------------------------------------------------------------------===//
// Copy Local State Declaration
//===--------------------------------------------------------------------===//
class PartitionedCopyLocalState;

class CopyToFileLocalState : public LocalSinkState {
public:
	CopyToFileLocalState(const PhysicalCopyToFile &op_p, ExecutionContext &context_p, CopyToFileGlobalState &gstate_p);

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
// Partitioned Copy Declarations
//===--------------------------------------------------------------------===//
enum class PartitionedCopyStage : uint8_t { SORT, MATERIALIZE, MASK, BATCH, PREPARE, FLUSH, DONE };
enum class FileCreationReason : uint8_t { NORMAL, SORTED_RUN_BOUNDARY, ROTATION };

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
	//! Serializes operations that need a complete partition writer run boundary.
	annotated_mutex lock;
	unique_ptr<GlobalFileState> file_state;
	idx_t active_writes = 0;
};

struct PartitionFileStateReservation {
	vector<unique_ptr<GlobalFileState>> files_to_finalize;
	idx_t offset = 0;
};

enum class PartitionedCopyBatchMode : uint8_t { BUFFERING, PREPARING, DELAYED, PREPARED };

enum class PartitionedCopyCollectionSchema : uint8_t { RAW_SCHEMA, WRITE_SCHEMA };

struct PartitionedCopyCollection {
	PartitionedCopyCollection() = default;
	explicit PartitionedCopyCollection(PartitionedCopyCollectionSchema schema_p) : schema(schema_p) {
	}
	PartitionedCopyCollection(PartitionedCopyCollectionSchema schema_p, unique_ptr<ColumnDataCollection> collection_p)
	    : schema(schema_p), collection(std::move(collection_p)) {
	}

	idx_t Count() const {
		return collection ? collection->Count() : 0;
	}

	PartitionedCopyCollectionSchema schema = PartitionedCopyCollectionSchema::WRITE_SCHEMA;
	unique_ptr<ColumnDataCollection> collection;
};

struct PartitionedCopyBatchState {
	void SetValues(vector<Value> values_p) {
		D_ASSERT(values.empty() || values == values_p);
		if (values.empty()) {
			values = std::move(values_p);
		}
	}

	idx_t AddCollectionSlot(PartitionedCopyCollectionSchema schema, idx_t row_count) {
		D_ASSERT(mode == PartitionedCopyBatchMode::BUFFERING || mode == PartitionedCopyBatchMode::PREPARING);
		collections.emplace_back(schema);
		if (mode == PartitionedCopyBatchMode::PREPARING && batches.size() < collections.size()) {
			batches.emplace_back();
		}
		count += row_count;
		return collections.size() - 1;
	}

	void StoreCollection(idx_t batch_idx, unique_ptr<ColumnDataCollection> collection) {
		D_ASSERT(mode == PartitionedCopyBatchMode::BUFFERING || mode == PartitionedCopyBatchMode::PREPARING);
		D_ASSERT(batch_idx < collections.size());
		collections[batch_idx].collection = std::move(collection);
	}

	bool CanStartPreparing(idx_t flush_threshold, bool has_delayed_partition) const {
		return mode == PartitionedCopyBatchMode::BUFFERING && count >= flush_threshold && !has_delayed_partition;
	}

	void StartPreparing() {
		D_ASSERT(mode == PartitionedCopyBatchMode::BUFFERING);
		mode = PartitionedCopyBatchMode::PREPARING;
		batches.resize(collections.size());
	}

	bool TryReserveWriteInfo() {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		if (write_info || write_info_requested) {
			return false;
		}
		write_info_requested = true;
		return true;
	}

	void SetWriteInfo(PartitionWriteInfo &write_info_p) {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		D_ASSERT(!write_info || write_info == optional_ptr<PartitionWriteInfo>(write_info_p));
		write_info = write_info_p;
		write_info_requested = false;
		batches.resize(collections.size());
	}

	void EnsurePreparingBatchSlots() {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		D_ASSERT(write_info);
		batches.resize(collections.size());
	}

	void MarkDelayed() {
		D_ASSERT(mode == PartitionedCopyBatchMode::BUFFERING);
		mode = PartitionedCopyBatchMode::DELAYED;
	}

	void MarkPrepared() {
		if (mode == PartitionedCopyBatchMode::PREPARING) {
			mode = PartitionedCopyBatchMode::PREPARED;
		}
	}

	bool SkipsPrepare() const {
		return mode == PartitionedCopyBatchMode::DELAYED || mode == PartitionedCopyBatchMode::PREPARED;
	}

	bool NeedsWriteInfo() const {
		return mode == PartitionedCopyBatchMode::PREPARING && !write_info;
	}

	bool IsWriteInfoReserved() const {
		return write_info_requested;
	}

	bool NeedsPrepare(idx_t batch_idx) const {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		D_ASSERT(write_info);
		D_ASSERT(batch_idx < collections.size());
		return collections[batch_idx].collection && (batch_idx >= batches.size() || !batches[batch_idx]);
	}

	PartitionedCopyCollection TakeCollection(idx_t batch_idx) {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		D_ASSERT(batch_idx < collections.size());
		return std::move(collections[batch_idx]);
	}

	void StorePreparedBatch(idx_t batch_idx, unique_ptr<PartitionedCopyBatch> batch) {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING || mode == PartitionedCopyBatchMode::PREPARED);
		D_ASSERT(batch_idx < batches.size());
		batches[batch_idx] = std::move(batch);
	}

	vector<PartitionedCopyCollection> TakeDelayedCollections() {
		D_ASSERT(mode == PartitionedCopyBatchMode::DELAYED);
		return std::move(collections);
	}

	vector<unique_ptr<PartitionedCopyBatch>> TakePreparedBatches() {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARED);
		return std::move(batches);
	}

	vector<Value> values;
	optional_ptr<PartitionWriteInfo> write_info;
	vector<PartitionedCopyCollection> collections;
	vector<unique_ptr<PartitionedCopyBatch>> batches;
	idx_t count = 0;
	bool write_info_requested = false;
	PartitionedCopyBatchMode mode = PartitionedCopyBatchMode::BUFFERING;
};

struct DelayedPartitionFlush {
	vector<Value> values;
	PartitionedCopyCollection data;
};

struct DelayedPartitionState {
	bool HasBufferedData() const {
		return buffer.Count() > 0;
	}

	bool InFlight() const {
		return in_flight > 0;
	}

	void Buffer(PartitionedCopyCollection data) {
		if (!HasBufferedData()) {
			buffer = std::move(data);
			return;
		}
		D_ASSERT(buffer.schema == data.schema);
		buffer.collection->Combine(*data.collection);
	}

	bool Ready(idx_t flush_threshold, bool force) const {
		return HasBufferedData() && !InFlight() && (force || buffer.Count() >= flush_threshold);
	}

	DelayedPartitionFlush Take(const vector<Value> &values) {
		D_ASSERT(HasBufferedData());
		D_ASSERT(!InFlight());
		DelayedPartitionFlush result;
		result.values = values;
		result.data = std::move(buffer);
		in_flight++;
		return result;
	}

	void Complete() {
		D_ASSERT(in_flight > 0);
		in_flight--;
	}

	bool Active() const {
		return HasBufferedData() || InFlight();
	}

	PartitionedCopyCollection buffer;
	idx_t in_flight = 0;
};

class DelayedPartitionBuffers {
public:
	bool Has(const vector<Value> &values) DUCKDB_EXCLUDES(lock) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return partitions.find(values) != partitions.end();
	}

	bool Empty() const DUCKDB_EXCLUDES(lock) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return partitions.empty();
	}

	optional<DelayedPartitionFlush> BufferOrTakeReady(const vector<Value> &values, PartitionedCopyCollection data,
	                                                  const idx_t flush_threshold, const bool force = false)
	    DUCKDB_EXCLUDES(lock) {
		if (!data.collection || data.collection->Count() == 0) {
			return nullopt;
		}

		DelayedPartitionFlush result;
		bool has_result = false;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			auto &state = partitions[values];
			state.Buffer(std::move(data));
			if (state.Ready(flush_threshold, force)) {
				result = state.Take(values);
				has_result = true;
			}
			EraseIfInactive(values, state);
		}

		if (!has_result) {
			return nullopt;
		}
		return std::move(result);
	}

	optional<DelayedPartitionFlush> TakeNext() DUCKDB_EXCLUDES(lock) {
		DelayedPartitionFlush result;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			auto next = partitions.end();
			for (auto entry = partitions.begin(); entry != partitions.end(); entry++) {
				auto &state = entry->second;
				if (!state.HasBufferedData() || state.InFlight()) {
					continue;
				}
				if (next == partitions.end() || VectorOfValuesLess()(entry->first, next->first)) {
					next = entry;
				}
			}
			if (next != partitions.end()) {
				result = next->second.Take(next->first);
				return std::move(result);
			}
		}
		return nullopt;
	}

	optional<DelayedPartitionFlush> Complete(const vector<Value> &values, const idx_t flush_threshold,
	                                         const bool take_ready) DUCKDB_EXCLUDES(lock) {
		DelayedPartitionFlush result;
		bool has_result = false;
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto entry = partitions.find(values);
		D_ASSERT(entry != partitions.end());
		if (entry == partitions.end()) {
			return nullopt;
		}
		entry->second.Complete();
		if (take_ready && entry->second.Ready(flush_threshold, false)) {
			// Data for the same partition reached the threshold while an earlier flush was in-flight.
			// Hand it back to the caller immediately so the partition does not wait for a global drain.
			result = entry->second.Take(values);
			has_result = true;
		} else if (!entry->second.Active()) {
			partitions.erase(entry);
		}
		if (!has_result) {
			return nullopt;
		}
		return std::move(result);
	}

private:
	void EraseIfInactive(const vector<Value> &values, const DelayedPartitionState &state) DUCKDB_REQUIRES(lock) {
		if (!state.Active()) {
			partitions.erase(values);
		}
	}

	mutable annotated_mutex lock;
	vector_of_value_map_t<DelayedPartitionState> partitions DUCKDB_GUARDED_BY(lock);
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
	optional<PartitionedCopyTask> TryNextPrepareTask() DUCKDB_REQUIRES(lock);
	optional<PartitionedCopyTask> TryNextFlushTask() DUCKDB_REQUIRES(lock);

	void Sort(ExecutionContext &context, GlobalSinkState &sink, InterruptState &interrupt,
	          const PartitionedCopyTask &task);
	void Materialize(ExecutionContext &context, GlobalSourceState &source, InterruptState &interrupt,
	                 const PartitionedCopyTask &task);
	void Mask(const PartitionedCopyTask &task);
	void Batch(const PartitionedCopyTask &task);
	void Prepare(ExecutionContext &execution_context, InterruptState &interrupt_state, const PartitionedCopyTask &task);
	void Flush(ExecutionContext &execution_context, InterruptState &interrupt_state, const PartitionedCopyTask &task);
	void PrepareBatchStates() DUCKDB_REQUIRES(lock);
	void CompletePreparedBatchStates() DUCKDB_REQUIRES(lock);

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
	//! Count of rows that either have a prepared batch or do not need one
	atomic<idx_t> prepared;

	//! The current partition/batch index for preparing
	idx_t prepare_partition_idx DUCKDB_GUARDED_BY(lock) = 0;
	idx_t prepare_batch_idx DUCKDB_GUARDED_BY(lock) = 0;

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
	bool ShouldInitiateFlush(const idx_t &local_append_count);
	bool IsCombineComplete() const DUCKDB_REQUIRES(lock);
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

	//! To estimate number of partitions
	ParallelHyperLogLogGlobalState hll;
	//! Check if we should flush once a thread's append count exceeds this value
	atomic<idx_t> next_flush_check;

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

enum class PartitionedCopyCombineType : uint8_t { DURING_SINK, DURING_PIPELINE_COMBINE };

//! Manages partitioned COPY
class PartitionedCopy {
public:
	PartitionedCopy(const PhysicalCopyToFile &op, ClientContext &context, CopyToFileGlobalState &copy_gstate);

public:
	//! PhysicalOperator-like interface
	void Sink(ExecutionContext &execution_context, DataChunk &chunk, PartitionedCopyLocalState &lstate,
	          InterruptState &interrupt_state);
	void Combine(ExecutionContext &execution_context, PartitionedCopyLocalState &lstate,
	             InterruptState &interrupt_state, PartitionedCopyCombineType combine_type);
	void Finalize(Pipeline &pipeline, Event &event, InterruptState &interrupt_state);
	void Flush(ExecutionContext &execution_context, InterruptState &interrupt_state);

	void InitializeFlush() DUCKDB_REQUIRES(lock);
	void FinalizeState(PartitionedCopyState &state, InterruptState &interrupt_state) DUCKDB_REQUIRES(state.lock);

	PartitionWriteInfo &GetPartitionWriteInfo(const vector<Value> &values) DUCKDB_EXCLUDES(active_writes_lock);
	void ReleasePartitionWriteInfo(PartitionWriteInfo &write_info) DUCKDB_EXCLUDES(active_writes_lock);
	bool HasDelayedPartition(const vector<Value> &values);
	bool HasDelayedPartitions() const;
	optional<DelayedPartitionFlush> BufferOrTakeReadyPartition(const vector<Value> &values,
	                                                           PartitionedCopyCollection data, bool force = false);
	optional<DelayedPartitionFlush> TakeNextDelayedPartition();
	optional<DelayedPartitionFlush> CompleteDelayedPartition(const vector<Value> &values, bool take_ready);
	bool DrainDelayedPartitions(ExecutionContext &execution_context, InterruptState &interrupt_state);
	unique_ptr<ColumnDataCollection> SortPartitionCollection(ExecutionContext &execution_context,
	                                                         InterruptState &interrupt_state,
	                                                         unique_ptr<ColumnDataCollection> collection);
	PartitionedCopyCollectionSchema GetPartitionCollectionSchema() const;
	const vector<LogicalType> &GetPartitionCollectionTypes(PartitionedCopyCollectionSchema schema) const;
	unique_ptr<ColumnDataCollection> PrepareCollectionForWrite(PartitionedCopyCollection data);
	unique_ptr<ColumnDataCollection> ProjectToWriteColumns(unique_ptr<ColumnDataCollection> collection);
	unique_ptr<PartitionedCopyBatch> PreparePartitionBatch(const vector<Value> &values, PartitionWriteInfo &write_info,
	                                                       PartitionedCopyCollection data);
	void FlushPreparedPartitionRun(const vector<Value> &values, PartitionWriteInfo &write_info,
	                               vector<unique_ptr<PartitionedCopyBatch>> batches);
	void FlushPreparedPartitionBatch(const vector<Value> &values, PartitionWriteInfo &write_info,
	                                 unique_ptr<PartitionedCopyBatch> batch);
	void FlushPartitionCollection(ExecutionContext &execution_context, InterruptState &interrupt_state,
	                              DelayedPartitionFlush flush);
	unique_ptr<GlobalFileState> CreatePartitionFileState(const vector<Value> &values,
	                                                     FileCreationReason reason = FileCreationReason::NORMAL)
	    DUCKDB_EXCLUDES(active_writes_lock);
	PartitionFileStateReservation
	ReservePartitionFileStateLocked(const vector<Value> &values, FileCreationReason reason = FileCreationReason::NORMAL)
	    DUCKDB_REQUIRES(active_writes_lock);
	unique_ptr<GlobalFileState> CreatePartitionFileStateFromReservation(const vector<Value> &values, idx_t offset)
	    DUCKDB_EXCLUDES(active_writes_lock);
	void FinalizeActiveWrites() DUCKDB_EXCLUDES(active_writes_lock);
	void FinalizeFileStates(vector<unique_ptr<GlobalFileState>> files_to_finalize) DUCKDB_EXCLUDES(active_writes_lock);
	string GetOrCreateDirectory(string path, const vector<Value> &values) DUCKDB_REQUIRES(copy_gstate.lock);

private:
	unique_ptr<const SortStrategy> ConstructSortStrategy() const;
	void CreateNextState();
	bool ShouldStopFlushing() const;
	bool RequiresSerializedPartitionWrites() const;
	void EnsureFreshPartitionFileForSortedRun(PartitionWriteInfo &write_info, const vector<Value> &values)
	    DUCKDB_EXCLUDES(active_writes_lock);
	void EnsureFreshPartitionFileForRotation(PartitionWriteInfo &write_info, const vector<Value> &values)
	    DUCKDB_EXCLUDES(active_writes_lock);
	//! Swaps write_info.file_state after temporarily dropping copy_gstate.lock to initialize the replacement file.
	//! Callers that can reach the swap path must serialize the full partition writer run for this write_info.
	void EnsureFreshPartitionFile(PartitionWriteInfo &write_info, const vector<Value> &values,
	                              FileCreationReason reason) DUCKDB_EXCLUDES(active_writes_lock);
	template <class FUNC>
	void WithSerializedPartitionWriteRun(PartitionWriteInfo &write_info, FUNC &&func) {
		annotated_unique_lock<annotated_mutex> run_guard(write_info.lock, std::defer_lock);
		if (RequiresSerializedPartitionWrites()) {
			run_guard.lock();
		}
		func();
	}
	void FlushDelayedPartitionRun(const vector<Value> &values, PartitionWriteInfo &write_info,
	                              ColumnDataCollection &collection);

public:
	const PhysicalCopyToFile &op;
	ClientContext &context;
	CopyToFileGlobalState &copy_gstate;

	//! Which columns/types to write to the file
	vector<column_t> write_columns;
	vector<LogicalType> write_types;
	vector<column_t> raw_columns;

	//! Partition/sort strategy with PhysicalOperator-like interface
	const unique_ptr<const SortStrategy> sort_strategy;

	//! Lock for managing states (sinking_state, flushing_state, flushing flag)
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

	//! Fine-grained lock for partition write tracking
	mutable annotated_mutex active_writes_lock;
	//! The active writes per partition (for partitioned write)
	vector_of_value_map_t<unique_ptr<PartitionWriteInfo>> active_writes DUCKDB_GUARDED_BY(active_writes_lock);
	vector_of_value_map_t<idx_t> previous_partitions DUCKDB_GUARDED_BY(active_writes_lock);
	idx_t global_offset DUCKDB_GUARDED_BY(active_writes_lock) = 0;

	//! Delayed below-threshold partitions
	DelayedPartitionBuffers delayed_partition_buffers;
};

//===--------------------------------------------------------------------===//
// Partitioned Copy Scoped Guards
//===--------------------------------------------------------------------===//
class PartitionWriteInfoGuard {
public:
	PartitionWriteInfoGuard(PartitionedCopy &partitioned_copy_p, PartitionWriteInfo &write_info_p)
	    : partitioned_copy(partitioned_copy_p), write_info(write_info_p) {
	}

	~PartitionWriteInfoGuard();

private:
	PartitionedCopy &partitioned_copy;
	optional_ptr<PartitionWriteInfo> write_info;
};

class DelayedPartitionFlushGuard {
public:
	DelayedPartitionFlushGuard(PartitionedCopy &partitioned_copy_p, const vector<Value> &values_p)
	    : partitioned_copy(partitioned_copy_p), values(values_p) {
	}

	~DelayedPartitionFlushGuard();
	optional<DelayedPartitionFlush> Complete();

private:
	PartitionedCopy &partitioned_copy;
	const vector<Value> &values;
	bool active = true;
};

PartitionWriteInfoGuard::~PartitionWriteInfoGuard() {
	if (write_info) {
		partitioned_copy.ReleasePartitionWriteInfo(*write_info);
	}
}

DelayedPartitionFlushGuard::~DelayedPartitionFlushGuard() {
	if (active) {
		partitioned_copy.CompleteDelayedPartition(values, false);
	}
}

optional<DelayedPartitionFlush> DelayedPartitionFlushGuard::Complete() {
	if (!active) {
		return nullopt;
	}
	active = false;
	return partitioned_copy.CompleteDelayedPartition(values, true);
}

//===--------------------------------------------------------------------===//
// Partitioned Hash Group Implementation
//===--------------------------------------------------------------------===//
PartitionedCopyHashGroup::PartitionedCopyHashGroup(PartitionedCopy &partitioned_copy, const ChunkRow &chunk_row,
                                                   idx_t group_idx_p)
    : partitioned_copy(partitioned_copy), count(chunk_row.count), blocks(chunk_row.chunks), group_idx(group_idx_p),
      stage(PartitionedCopyStage::SORT), sorted(0), materialized(0), masked(0), batched(0), prepared(0), flushed(0) {
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
			PrepareBatchStates();
			stage = PartitionedCopyStage::PREPARE;
			return true;
		}
		return false;
	case PartitionedCopyStage::PREPARE:
		if (prepared == count) {
			CompletePreparedBatchStates();
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
		case PartitionedCopyStage::PREPARE:
			return TryNextPrepareTask();
		case PartitionedCopyStage::FLUSH:
			return TryNextFlushTask();
		default:
			return nullopt;
		}
	}
	const auto task_stage = static_cast<PartitionedCopyStage>(next_task / group_threads);
	if (task_stage != group_stage) {
		return nullopt;
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
		return nullopt;
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
	task.batch_idx = batch_state.collections.size();
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
	const auto batch_idx =
	    batch_state.AddCollectionSlot(partitioned_copy.GetPartitionCollectionSchema(), task.end_idx - task.begin_idx);
	D_ASSERT(batch_idx == task.batch_idx);

	return task;
}

optional<PartitionedCopyTask> PartitionedCopyHashGroup::TryNextPrepareTask() {
	while (prepare_partition_idx < batch_states.size()) {
		auto &batch_state = *batch_states[prepare_partition_idx];
		if (batch_state.SkipsPrepare()) {
			++prepare_partition_idx;
			prepare_batch_idx = 0;
			continue;
		}
		D_ASSERT(batch_state.mode == PartitionedCopyBatchMode::PREPARING);
		if (batch_state.NeedsWriteInfo()) {
			if (!batch_state.IsWriteInfoReserved() && batch_state.TryReserveWriteInfo()) {
				PartitionedCopyTask task;
				task.stage = PartitionedCopyStage::PREPARE;
				task.group_idx = group_idx;
				task.thread_idx = prepare_partition_idx;
				task.batch_idx = DConstants::INVALID_INDEX;
				return task;
			}
			return nullopt;
		}
		while (prepare_batch_idx < batch_state.collections.size()) {
			if (!batch_state.NeedsPrepare(prepare_batch_idx)) {
				++prepare_batch_idx;
				continue;
			}
			break;
		}
		if (prepare_batch_idx == batch_state.collections.size()) {
			++prepare_partition_idx;
			prepare_batch_idx = 0;
			continue;
		}

		PartitionedCopyTask task;
		task.stage = PartitionedCopyStage::PREPARE;
		task.group_idx = group_idx;
		task.thread_idx = prepare_partition_idx;
		task.batch_idx = prepare_batch_idx++;

		return task;
	}

	return nullopt;
}

optional<PartitionedCopyTask> PartitionedCopyHashGroup::TryNextFlushTask() {
	if (flush_partition_idx == batch_states.size()) {
		return nullopt;
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
	// Read `blocks` before incrementing `materialized`: once materialized == blocks another thread can advance
	// the stage all the way to FLUSH completion and delete this hash group, making `blocks` a dangling read.
	const idx_t local_blocks = blocks;
	const auto new_materialized = (materialized += (task.end_idx - task.begin_idx));

	if (new_materialized >= local_blocks) {
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
	const auto collection_schema = partitioned_copy.GetPartitionCollectionSchema();
	const auto &batch_types = partitioned_copy.GetPartitionCollectionTypes(collection_schema);
	const auto &batch_columns = collection_schema == PartitionedCopyCollectionSchema::RAW_SCHEMA
	                                ? partitioned_copy.raw_columns
	                                : partitioned_copy.write_columns;

	// Initialize the scan
	ColumnDataScanState scan_state;
	collection->InitializeScan(scan_state);
	DataChunk scan_chunk;
	collection->InitializeScanChunk(scan_state, scan_chunk);
	collection->Seek(task.begin_idx, scan_state, scan_chunk);
	D_ASSERT(task.begin_idx >= scan_state.current_row_index);

	// Initialize the batch chunk
	DataChunk batch_chunk;
	batch_chunk.Initialize(partitioned_copy.context, batch_types);

	// Initialize the append
	auto batch = make_uniq<ColumnDataCollection>(partitioned_copy.context, batch_types);
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

		batch_chunk.ReferenceColumns(scan_chunk, batch_columns);
		batch->Append(append_state, batch_chunk);
		collection->Scan(scan_state, scan_chunk);
	}

	// Get pointer to batch state (under lock)
	optional_ptr<PartitionedCopyBatchState> batch_state;
	optional_ptr<PartitionWriteInfo> write_info;
	vector<Value> partition_values;
	bool acquire_write_info = false;
	bool prepare_batch = false;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		batch_state = batch_states[task.thread_idx];
		batch_state->SetValues(std::move(values));

		if (batch_state->mode == PartitionedCopyBatchMode::BUFFERING) {
			const auto flush_threshold = Settings::Get<PartitionedWriteFlushThresholdSetting>(partitioned_copy.context);
			const auto has_delayed_partition = partitioned_copy.HasDelayedPartition(batch_state->values);
			if (batch_state->CanStartPreparing(flush_threshold, has_delayed_partition)) {
				batch_state->StartPreparing();
				acquire_write_info = batch_state->TryReserveWriteInfo();
			}
		}

		if (batch_state->mode == PartitionedCopyBatchMode::PREPARING && batch_state->write_info) {
			write_info = batch_state->write_info;
			partition_values = batch_state->values;
			prepare_batch = true;
		} else if (acquire_write_info) {
			partition_values = batch_state->values;
			prepare_batch = true;
		}
	}

	const auto row_count = task.end_idx - task.begin_idx;
	if (!prepare_batch) {
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			auto &current_batch_state = *batch_states[task.thread_idx];
			current_batch_state.StoreCollection(task.batch_idx, std::move(batch));
		}
		batched += row_count;
		return;
	}

	if (acquire_write_info) {
		auto &partition_write_info = partitioned_copy.GetPartitionWriteInfo(partition_values);
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			auto &current_batch_state = *batch_states[task.thread_idx];
			current_batch_state.SetWriteInfo(partition_write_info);
			write_info = current_batch_state.write_info;
		}
	}
	D_ASSERT(write_info);
	auto prepared_batch = partitioned_copy.PreparePartitionBatch(
	    partition_values, *write_info, PartitionedCopyCollection(collection_schema, std::move(batch)));

	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &current_batch_state = *batch_states[task.thread_idx];
		current_batch_state.StorePreparedBatch(task.batch_idx, std::move(prepared_batch));
	}

	prepared += row_count;
	batched += row_count;
}

void PartitionedCopyHashGroup::PrepareBatchStates() {
	const auto flush_threshold = Settings::Get<PartitionedWriteFlushThresholdSetting>(partitioned_copy.context);
	for (auto &batch_state : batch_states) {
		D_ASSERT(batch_state);
		D_ASSERT(!batch_state->values.empty());
		D_ASSERT(batch_state->count > 0);

		if (batch_state->mode == PartitionedCopyBatchMode::PREPARING) {
			D_ASSERT(batch_state->write_info);
			batch_state->EnsurePreparingBatchSlots();
			continue;
		}
		D_ASSERT(batch_state->mode == PartitionedCopyBatchMode::BUFFERING);

		if (batch_state->count < flush_threshold || partitioned_copy.HasDelayedPartition(batch_state->values)) {
			batch_state->MarkDelayed();
			prepared += batch_state->count;
			continue;
		}

		batch_state->StartPreparing();
	}
}

void PartitionedCopyHashGroup::CompletePreparedBatchStates() {
	for (auto &batch_state : batch_states) {
		D_ASSERT(batch_state);
		batch_state->MarkPrepared();
	}
}

void PartitionedCopyHashGroup::Prepare(ExecutionContext &execution_context, InterruptState &interrupt_state,
                                       const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::PREPARE);

	vector<Value> values;
	optional_ptr<PartitionWriteInfo> write_info;
	PartitionedCopyCollection data;
	bool acquire_write_info = false;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &batch_state = *batch_states[task.thread_idx];
		D_ASSERT(batch_state.mode == PartitionedCopyBatchMode::PREPARING);
		values = batch_state.values;
		if (task.batch_idx == DConstants::INVALID_INDEX) {
			D_ASSERT(batch_state.NeedsWriteInfo());
			D_ASSERT(batch_state.IsWriteInfoReserved());
			acquire_write_info = true;
		} else {
			D_ASSERT(batch_state.write_info);
			D_ASSERT(task.batch_idx < batch_state.collections.size());
			write_info = batch_state.write_info;
			data = batch_state.TakeCollection(task.batch_idx);
		}
	}

	if (acquire_write_info) {
		auto &partition_write_info = partitioned_copy.GetPartitionWriteInfo(values);
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &batch_state = *batch_states[task.thread_idx];
		batch_state.SetWriteInfo(partition_write_info);
		return;
	}
	D_ASSERT(data.collection);
	const auto row_count = data.Count();

	auto prepared_batch = partitioned_copy.PreparePartitionBatch(values, *write_info, std::move(data));

	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &batch_state = *batch_states[task.thread_idx];
		batch_state.StorePreparedBatch(task.batch_idx, std::move(prepared_batch));
	}

	prepared += row_count;
}

void PartitionedCopyHashGroup::Flush(ExecutionContext &execution_context, InterruptState &interrupt_state,
                                     const PartitionedCopyTask &task) {
	D_ASSERT(task.stage == PartitionedCopyStage::FLUSH);

	vector<Value> values;
	vector<PartitionedCopyCollection> collections;
	vector<unique_ptr<PartitionedCopyBatch>> batches;
	optional_ptr<PartitionWriteInfo> write_info;
	PartitionedCopyBatchMode mode;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &batch_state = *batch_states[task.thread_idx];
		values = batch_state.values;
		mode = batch_state.mode;
		if (mode == PartitionedCopyBatchMode::DELAYED) {
			collections = batch_state.TakeDelayedCollections();
		} else {
			D_ASSERT(mode == PartitionedCopyBatchMode::PREPARED);
			write_info = batch_state.write_info;
			batches = batch_state.TakePreparedBatches();
		}
	}
	D_ASSERT(!values.empty());

	if (mode == PartitionedCopyBatchMode::DELAYED) {
		const auto collection_schema = partitioned_copy.GetPartitionCollectionSchema();
		const auto &collection_types = partitioned_copy.GetPartitionCollectionTypes(collection_schema);
		auto collection = make_uniq<ColumnDataCollection>(partitioned_copy.context, collection_types);
		for (auto &batch : collections) {
			D_ASSERT(batch.schema == collection_schema);
			if (batch.collection) {
				collection->Combine(*batch.collection);
			}
		}
		if (collection->Count() == 0) {
			return;
		}

		auto data = PartitionedCopyCollection(collection_schema, std::move(collection));
		auto ready_partition = partitioned_copy.BufferOrTakeReadyPartition(values, std::move(data), false);
		if (ready_partition) {
			partitioned_copy.FlushPartitionCollection(execution_context, interrupt_state, std::move(*ready_partition));
		}
		return;
	}

	D_ASSERT(write_info);
	PartitionWriteInfoGuard write_guard(partitioned_copy, *write_info);
	partitioned_copy.FlushPreparedPartitionRun(values, *write_info, std::move(batches));
}

//===--------------------------------------------------------------------===//
// Partitioned Copy State Implementation
//===--------------------------------------------------------------------===//
PartitionedCopyState::PartitionedCopyState(PartitionedCopy &partitioned_copy_p,
                                           unique_ptr<GlobalSinkState> global_sink_state_p)
    : partitioned_copy(partitioned_copy_p),
      next_flush_check(Settings::Get<PartitionedWriteFlushThresholdSetting>(partitioned_copy.context)),
      global_sink_state(std::move(global_sink_state_p)), total_blocks(0), next_group(0), locals(0), combined(0) {
}

bool PartitionedCopyState::ShouldInitiateFlush(const idx_t &local_append_count) {
	auto expected = next_flush_check.load(std::memory_order_relaxed);
	if (local_append_count < expected) {
		return false; // Not enough rows accumulated yet
	}

	// CAS so only one thread increments "next_flush_check"
	const auto flush_threshold = Settings::Get<PartitionedWriteFlushThresholdSetting>(partitioned_copy.context);
	const auto desired = expected + flush_threshold;
	const auto exchanged = next_flush_check.compare_exchange_strong(expected, desired, std::memory_order_relaxed,
	                                                                std::memory_order_relaxed);
	if (!exchanged) {
		return false; // Another thread beat us to it
	}

	// Get counts from the HLL states
	const auto merged_state = hll.GetMergedState();
	auto [unique_count, total_count] = merged_state->GetCounts();
	unique_count = MaxValue<idx_t>(unique_count, 1);

	// If we have enough rows per partition on average, we can start flushing
	return total_count / unique_count >= 2 * flush_threshold;
}

bool PartitionedCopyState::IsCombineComplete() const {
	return combined == locals;
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

	return nullopt;
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
	case PartitionedCopyStage::PREPARE:
		hash_group.Prepare(execution_context, interrupt, task);
		break;
	case PartitionedCopyStage::FLUSH:
		hash_group.Flush(execution_context, interrupt, task);
		break;
	default:
		throw InternalException("Invalid PartitionedCopyStage in PartitionedCopyState::ExecuteTask");
	}
	FinishTask(task);
}

void PartitionedCopyState::FinishTask(const PartitionedCopyTask &task) {
	const auto group_idx = task.group_idx;
	auto &finished_hash_group = hash_groups[group_idx];

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

//===--------------------------------------------------------------------===//
// Partitioned Copy Lifecycle
//===--------------------------------------------------------------------===//
PartitionedCopy::PartitionedCopy(const PhysicalCopyToFile &op_p, ClientContext &context_p,
                                 CopyToFileGlobalState &copy_gstate_p)
    : op(op_p), context(context_p), copy_gstate(copy_gstate_p), sort_strategy(ConstructSortStrategy()), flushing(false),
      locals(0), combined(0), finalized(false) {
	unordered_set<idx_t> part_col_set(op.partition_columns.begin(), op.partition_columns.end());
	for (idx_t col_idx = 0; col_idx < op.expected_types.size(); col_idx++) {
		raw_columns.push_back(col_idx);
		if (op.write_partition_columns || part_col_set.find(col_idx) == part_col_set.end()) {
			write_columns.push_back(col_idx);
			write_types.push_back(op.expected_types[col_idx]);
		}
	}
}

unique_ptr<const SortStrategy> PartitionedCopy::ConstructSortStrategy() const {
	vector<unique_ptr<Expression>> partition_bys;
	for (auto &col : op.partition_columns) {
		partition_bys.push_back(make_uniq<BoundReferenceExpression>(op.expected_types[col], col));
	}
	vector<unique_ptr<BaseStatistics>> partition_stats;

	return SortStrategy::Factory(context, partition_bys, op.order_columns, op.expected_types, partition_stats,
	                             op.children.empty() ? 0 : op.children[0].get().estimated_cardinality);
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

bool PartitionedCopy::RequiresSerializedPartitionWrites() const {
	// A full partition writer run must remain serialized when the run boundary has file-state semantics:
	// ORDER BY starts a fresh file per sorted run, and rotation can start a fresh file between batches.
	return !op.order_columns.empty() || op.Rotate();
}

void PartitionedCopy::InitializeFlush() {
	if (flushing) {
		return;
	}

	// Move current sinking state to flushing state
	flushing_state = std::move(sinking_state);
	if (!flushing_state) {
		return; // Nothing to do
	}

	flushing = true;
}

void PartitionedCopy::FinalizeState(PartitionedCopyState &state, InterruptState &interrupt_state) {
	D_ASSERT(state.combined == state.locals);
	OperatorSinkFinalizeInput sort_strategy_finalize_input {*state.global_sink_state, interrupt_state};
	sort_strategy->Finalize(context, sort_strategy_finalize_input);
	state.CreateTaskList();
}

void PartitionedCopy::Sink(ExecutionContext &execution_context, DataChunk &chunk, PartitionedCopyLocalState &lstate,
                           InterruptState &interrupt_state) {
	// Create new sinking/local state if necessary
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
		sort_strategy->RegisterHyperLogLog(*lstate.sort_strategy_local_state,
		                                   lstate.current_state->hll.GetLocalState());
		lstate.append_count = 0;
	}

	// Sink into sort strategy
	OperatorSinkInput sort_strategy_sink_input {*lstate.current_state->global_sink_state,
	                                            *lstate.sort_strategy_local_state, interrupt_state};
	sort_strategy->Sink(execution_context, chunk, sort_strategy_sink_input);
	lstate.append_count += chunk.size();

	if (!flushing.load(std::memory_order_relaxed) && lstate.current_state->ShouldInitiateFlush(lstate.append_count)) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		InitializeFlush();
	}

	if (!flushing.load(std::memory_order_relaxed)) {
		return;
	}

	Combine(execution_context, lstate, interrupt_state, PartitionedCopyCombineType::DURING_SINK);
	Flush(execution_context, interrupt_state);
}

void PartitionedCopy::Combine(ExecutionContext &execution_context, PartitionedCopyLocalState &lstate,
                              InterruptState &interrupt_state, PartitionedCopyCombineType combine_type) {
	if (!lstate.current_state) {
		return;
	}

	if (combine_type == PartitionedCopyCombineType::DURING_PIPELINE_COMBINE) {
		OperatorSinkCombineInput sort_strategy_combine_input {*lstate.current_state->global_sink_state,
		                                                      *lstate.sort_strategy_local_state, interrupt_state};
		sort_strategy->Combine(execution_context, sort_strategy_combine_input);

		annotated_lock_guard<annotated_mutex> guard(lstate.current_state->lock);
		++lstate.current_state->combined;

		return;
	}
	D_ASSERT(combine_type == PartitionedCopyCombineType::DURING_SINK);

	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!flushing_state || !RefersToSameObject(*lstate.current_state, *flushing_state)) {
			return; // Not flushing anymore, or can't combine this state into the flushing state
		}
	}

	OperatorSinkCombineInput sort_strategy_combine_input {*lstate.current_state->global_sink_state,
	                                                      *lstate.sort_strategy_local_state, interrupt_state};
	sort_strategy->Combine(execution_context, sort_strategy_combine_input);

	{
		// Finalize if this is the last combine
		annotated_lock_guard<annotated_mutex> guard(lstate.current_state->lock);
		if (++lstate.current_state->combined == lstate.current_state->locals) {
			FinalizeState(*lstate.current_state, interrupt_state);
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
		while (partitioned_copy.flushing.load(std::memory_order_relaxed) || partitioned_copy.HasDelayedPartitions()) {
			event->GetClientContext().InterruptCheck();
			if (partitioned_copy.flushing.load(std::memory_order_relaxed)) {
				partitioned_copy.Flush(execution_context, interrupt_state);
			} else {
				if (!partitioned_copy.DrainDelayedPartitions(execution_context, interrupt_state)) {
					TaskScheduler::YieldThread();
				}
			}
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
		idx_t task_count;
		if (partitioned_copy.flushing_state) {
			auto &flushing_state = *partitioned_copy.flushing_state;
			annotated_lock_guard<annotated_mutex> state_guard(flushing_state.lock);
			D_ASSERT(flushing_state.combined == flushing_state.locals);
			task_count = flushing_state.locals;
		} else {
			task_count = MaxValue<idx_t>(partitioned_copy.locals.load(std::memory_order_relaxed), 1);
		}

		vector<shared_ptr<Task>> tasks;
		for (idx_t i = 0; i < task_count; i++) {
			tasks.push_back(
			    make_shared_ptr<PartitionedCopyFinalizeTask>(GetClientContext(), shared_from_this(), partitioned_copy));
		}
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		bool done;
		{
			annotated_lock_guard<annotated_mutex> global_guard(partitioned_copy.lock);
			done = !partitioned_copy.sinking_state;
			if (!done) {
				auto partitioned_copy_finalize_event =
				    make_shared_ptr<PartitionedCopyFinalizeEvent>(*pipeline, partitioned_copy);
				InsertEvent(std::move(partitioned_copy_finalize_event));
			}
		}

		if (done) {
			partitioned_copy.FinalizeActiveWrites();
			partitioned_copy.copy_gstate.TryFinalizeOwnedFileState();
		}
	}

private:
	PartitionedCopy &partitioned_copy;
};

void PartitionedCopy::Finalize(Pipeline &pipeline, Event &event, InterruptState &interrupt_state) {
	bool should_finalize_writes;
	{
		annotated_lock_guard<annotated_mutex> global_guard(lock);
		finalized = true;
		if (!sinking_state && !flushing_state && !HasDelayedPartitions()) {
			should_finalize_writes = true;
		} else {
			should_finalize_writes = false;
			if (sinking_state) {
				annotated_lock_guard<annotated_mutex> guard(sinking_state->lock);
				FinalizeState(*sinking_state, interrupt_state);
			}

			auto partitioned_copy_finalize_event = make_shared_ptr<PartitionedCopyFinalizeEvent>(pipeline, *this);
			event.InsertEvent(std::move(partitioned_copy_finalize_event));
		}
	}

	if (should_finalize_writes) {
		FinalizeActiveWrites();
		copy_gstate.TryFinalizeOwnedFileState();
	}
}

void PartitionedCopy::Flush(ExecutionContext &execution_context, InterruptState &interrupt_state) {
	shared_ptr<PartitionedCopyState> flushing_state_copy;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		flushing_state_copy = flushing_state;
	}

	if (!flushing_state_copy) {
		return; // Finalization not yet complete, nothing to do
	}

	{
		annotated_lock_guard<annotated_mutex> guard(flushing_state_copy->lock);
		D_ASSERT(flushing_state_copy->combined <= flushing_state_copy->locals);
		if (!flushing_state_copy->global_source_state) {
			if (!flushing_state_copy->IsCombineComplete()) {
				return; // Combine not complete yet
			}
			D_ASSERT(flushing_state_copy->IsCombineComplete());
			FinalizeState(*flushing_state_copy, interrupt_state);
			if (!flushing_state_copy->global_source_state) {
				return; // Finalization not yet complete, nothing to do
			}
		}
		D_ASSERT(flushing_state_copy->global_source_state);
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

	bool should_finalize_writes;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!flushing || !RefersToSameObject(*flushing_state, *flushing_state_copy)) {
			return;
		}
		flushing = false;
		flushing_state.reset();
		should_finalize_writes = finalized && !sinking_state;
	}
	if (should_finalize_writes) {
		DrainDelayedPartitions(execution_context, interrupt_state);
	}
}

//===--------------------------------------------------------------------===//
// Delayed Partition Buffering
//===--------------------------------------------------------------------===//
bool PartitionedCopy::HasDelayedPartition(const vector<Value> &values) {
	return delayed_partition_buffers.Has(values);
}

bool PartitionedCopy::HasDelayedPartitions() const {
	return !delayed_partition_buffers.Empty();
}

optional<DelayedPartitionFlush>
PartitionedCopy::BufferOrTakeReadyPartition(const vector<Value> &values, PartitionedCopyCollection data, bool force) {
	const auto flush_threshold = Settings::Get<PartitionedWriteFlushThresholdSetting>(context);
	return delayed_partition_buffers.BufferOrTakeReady(values, std::move(data), flush_threshold, force);
}

optional<DelayedPartitionFlush> PartitionedCopy::TakeNextDelayedPartition() {
	return delayed_partition_buffers.TakeNext();
}

optional<DelayedPartitionFlush> PartitionedCopy::CompleteDelayedPartition(const vector<Value> &values,
                                                                          const bool take_ready) {
	const auto flush_threshold = Settings::Get<PartitionedWriteFlushThresholdSetting>(context);
	return delayed_partition_buffers.Complete(values, flush_threshold, take_ready);
}

bool PartitionedCopy::DrainDelayedPartitions(ExecutionContext &execution_context, InterruptState &interrupt_state) {
	bool drained = false;
	while (auto partition = TakeNextDelayedPartition()) {
		drained = true;
		FlushPartitionCollection(execution_context, interrupt_state, std::move(*partition));
	}
	return drained;
}

//===--------------------------------------------------------------------===//
// Partitioned Collection Helpers
//===--------------------------------------------------------------------===//
unique_ptr<ColumnDataCollection> PartitionedCopy::SortPartitionCollection(ExecutionContext &execution_context,
                                                                          InterruptState &interrupt_state,
                                                                          unique_ptr<ColumnDataCollection> collection) {
	D_ASSERT(collection);
	D_ASSERT(collection->Count() > 0);

	auto global_sink_state = sort_strategy->GetGlobalSinkState(context);
	auto local_sink_state = sort_strategy->GetLocalSinkState(execution_context);

	ColumnDataScanState scan_state;
	collection->InitializeScan(scan_state);
	DataChunk scan_chunk;
	collection->InitializeScanChunk(scan_state, scan_chunk);

	while (collection->Scan(scan_state, scan_chunk)) {
		OperatorSinkInput sink_input {*global_sink_state, *local_sink_state, interrupt_state};
		sort_strategy->Sink(execution_context, scan_chunk, sink_input);
	}

	OperatorSinkCombineInput combine_input {*global_sink_state, *local_sink_state, interrupt_state};
	sort_strategy->Combine(execution_context, combine_input);

	OperatorSinkFinalizeInput finalize_input {*global_sink_state, interrupt_state};
	sort_strategy->Finalize(context, finalize_input);

	auto global_source_state = sort_strategy->GetGlobalSourceState(context, *global_sink_state);
	const auto &hash_groups = sort_strategy->GetHashGroups(*global_source_state);

	auto local_source_state = sort_strategy->GetLocalSourceState(execution_context, *global_source_state);
	OperatorSourceInput source_input {*global_source_state, *local_source_state, interrupt_state};
	for (idx_t group_idx = 0; group_idx < hash_groups.size(); group_idx++) {
		if (!hash_groups[group_idx].count) {
			continue;
		}

		sort_strategy->SortColumnData(execution_context, group_idx, finalize_input);
		while (sort_strategy->MaterializeColumnData(execution_context, group_idx, source_input) ==
		       SourceResultType::HAVE_MORE_OUTPUT) {
		}

		auto result = sort_strategy->GetColumnData(group_idx, source_input);
		if (!result || result->Count() != collection->Count()) {
			throw InternalException("Failed to materialize delayed partition data");
		}
		return result;
	}

	throw InternalException("Failed to find delayed partition data after sorting");
}

PartitionedCopyCollectionSchema PartitionedCopy::GetPartitionCollectionSchema() const {
	return op.order_columns.empty() ? PartitionedCopyCollectionSchema::WRITE_SCHEMA
	                                : PartitionedCopyCollectionSchema::RAW_SCHEMA;
}

const vector<LogicalType> &PartitionedCopy::GetPartitionCollectionTypes(PartitionedCopyCollectionSchema schema) const {
	return schema == PartitionedCopyCollectionSchema::RAW_SCHEMA ? op.expected_types : write_types;
}

unique_ptr<ColumnDataCollection> PartitionedCopy::PrepareCollectionForWrite(PartitionedCopyCollection data) {
	D_ASSERT(data.collection);
	if (data.schema == PartitionedCopyCollectionSchema::WRITE_SCHEMA) {
		return std::move(data.collection);
	}
	return ProjectToWriteColumns(std::move(data.collection));
}

unique_ptr<ColumnDataCollection> PartitionedCopy::ProjectToWriteColumns(unique_ptr<ColumnDataCollection> collection) {
	D_ASSERT(collection);

	auto result = make_uniq<ColumnDataCollection>(context, write_types);
	ColumnDataAppendState append_state;
	result->InitializeAppend(append_state);

	ColumnDataScanState scan_state;
	collection->InitializeScan(scan_state);
	DataChunk scan_chunk;
	collection->InitializeScanChunk(scan_state, scan_chunk);

	DataChunk write_chunk;
	write_chunk.Initialize(context, write_types);

	while (collection->Scan(scan_state, scan_chunk)) {
		write_chunk.ReferenceColumns(scan_chunk, write_columns);
		result->Append(append_state, write_chunk);
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Partitioned Write Helpers
//===--------------------------------------------------------------------===//
unique_ptr<PartitionedCopyBatch> PartitionedCopy::PreparePartitionBatch(const vector<Value> &values,
                                                                        PartitionWriteInfo &write_info,
                                                                        PartitionedCopyCollection data) {
	auto collection = PrepareCollectionForWrite(std::move(data));
	const auto create_file_state_fun = [&]() {
		return CreatePartitionFileState(values);
	};
	auto [batch_analyzer, prepared_batch] =
	    op.PrepareBatch(context, copy_gstate, write_info.file_state, create_file_state_fun, std::move(collection));
	return make_uniq<PartitionedCopyBatch>(batch_analyzer, std::move(prepared_batch));
}

void PartitionedCopy::FlushPreparedPartitionRun(const vector<Value> &values, PartitionWriteInfo &write_info,
                                                vector<unique_ptr<PartitionedCopyBatch>> batches) {
	WithSerializedPartitionWriteRun(write_info, [&]() {
		EnsureFreshPartitionFileForSortedRun(write_info, values);
		for (auto &batch : batches) {
			D_ASSERT(batch);
			EnsureFreshPartitionFileForRotation(write_info, values);
			FlushPreparedPartitionBatch(values, write_info, std::move(batch));
		}
	});
}

void PartitionedCopy::FlushPreparedPartitionBatch(const vector<Value> &values, PartitionWriteInfo &write_info,
                                                  unique_ptr<PartitionedCopyBatch> batch) {
	D_ASSERT(batch);
	const auto create_file_state_fun = [&]() {
		return CreatePartitionFileState(values);
	};
	op.FlushBatch(context, copy_gstate, write_info.file_state, create_file_state_fun, batch->batch_analyzer,
	              std::move(batch->prepared_batch));
}

void PartitionedCopy::FlushDelayedPartitionRun(const vector<Value> &values, PartitionWriteInfo &write_info,
                                               ColumnDataCollection &collection) {
	WithSerializedPartitionWriteRun(write_info, [&]() {
		EnsureFreshPartitionFileForSortedRun(write_info, values);

		DataChunk scan_chunk;
		ColumnDataScanState scan_state;
		collection.InitializeScan(scan_state);
		collection.InitializeScanChunk(scan_state, scan_chunk);

		auto batch = make_uniq<ColumnDataCollection>(context, write_types);
		ColumnDataAppendState append_state;
		batch->InitializeAppend(append_state);

		const auto flush_batch = [&]() {
			if (batch->Count() == 0) {
				return;
			}
			EnsureFreshPartitionFileForRotation(write_info, values);
			auto prepared_batch = PreparePartitionBatch(
			    values, write_info,
			    PartitionedCopyCollection(PartitionedCopyCollectionSchema::WRITE_SCHEMA, std::move(batch)));
			FlushPreparedPartitionBatch(values, write_info, std::move(prepared_batch));

			batch = make_uniq<ColumnDataCollection>(context, write_types);
			batch->InitializeAppend(append_state);
		};

		while (collection.Scan(scan_state, scan_chunk)) {
			batch->Append(append_state, scan_chunk);
			const CopyFunctionBatchAnalyzer batch_analyzer(*batch, op.batch_size, op.batch_size_bytes);
			if (batch_analyzer.MeetsFlushCriteria()) {
				flush_batch();
			}
		}
		flush_batch();
	});
}

void PartitionedCopy::FlushPartitionCollection(ExecutionContext &execution_context, InterruptState &interrupt_state,
                                               DelayedPartitionFlush flush) {
	while (true) {
		DelayedPartitionFlushGuard delayed_guard(*this, flush.values);
		if (flush.data.collection && flush.data.collection->Count() > 0) {
			if (flush.data.schema == PartitionedCopyCollectionSchema::RAW_SCHEMA) {
				flush.data.collection =
				    SortPartitionCollection(execution_context, interrupt_state, std::move(flush.data.collection));
				flush.data.collection = ProjectToWriteColumns(std::move(flush.data.collection));
				flush.data.schema = PartitionedCopyCollectionSchema::WRITE_SCHEMA;
			}
			D_ASSERT(flush.data.schema == PartitionedCopyCollectionSchema::WRITE_SCHEMA);

			auto &write_info = GetPartitionWriteInfo(flush.values);
			PartitionWriteInfoGuard write_guard(*this, write_info);
			FlushDelayedPartitionRun(flush.values, write_info, *flush.data.collection);
		}

		auto next = delayed_guard.Complete();
		if (!next) {
			return;
		}
		flush = std::move(*next);
	}
}

PartitionWriteInfo &PartitionedCopy::GetPartitionWriteInfo(const vector<Value> &values) {
	PartitionWriteInfo *result;
	{
		annotated_lock_guard<annotated_mutex> guard(active_writes_lock);
		// check if we have already started writing this partition
		auto active_write_entry = active_writes.find(values);
		if (active_write_entry != active_writes.end()) {
			// we have - continue writing in this partition
			active_write_entry->second->active_writes++;
			result = active_write_entry->second.get();
		} else {
			auto info = make_uniq<PartitionWriteInfo>();
			result = info.get();

			info->active_writes = 1;
			// store in active write map
			active_writes.insert(make_pair(values, std::move(info)));
		}
	}

	return *result;
}

void PartitionedCopy::ReleasePartitionWriteInfo(PartitionWriteInfo &write_info) {
	annotated_lock_guard<annotated_mutex> guard(active_writes_lock);
	D_ASSERT(write_info.active_writes > 0);
	write_info.active_writes--;
}

unique_ptr<GlobalFileState> PartitionedCopy::CreatePartitionFileState(const vector<Value> &values,
                                                                      FileCreationReason reason) {
	PartitionFileStateReservation reservation;
	{
		annotated_lock_guard<annotated_mutex> guard(active_writes_lock);
		reservation = ReservePartitionFileStateLocked(values, reason);
	}
	FinalizeFileStates(std::move(reservation.files_to_finalize));
	return CreatePartitionFileStateFromReservation(values, reservation.offset);
}

PartitionFileStateReservation PartitionedCopy::ReservePartitionFileStateLocked(const vector<Value> &values,
                                                                               FileCreationReason reason) {
	PartitionFileStateReservation reservation;
	// check if we need to close any writers before we can continue
	if (active_writes.size() >= Settings::Get<PartitionedWriteMaxOpenFilesSetting>(context)) {
		// we need to! try to close writers
		for (auto it = active_writes.begin(); it != active_writes.end(); ++it) {
			if (it->second->active_writes == 0) {
				// we can evict this entry - evict the partition
				reservation.files_to_finalize.push_back(std::move(it->second->file_state));
				++previous_partitions[it->first];
				active_writes.erase(it);
				break;
			}
		}
	}

	if (op.hive_file_pattern) {
		if (reason == FileCreationReason::SORTED_RUN_BOUNDARY || reason == FileCreationReason::ROTATION) {
			++previous_partitions[values];
		}
		auto prev_offset = previous_partitions.find(values);
		if (prev_offset != previous_partitions.end()) {
			reservation.offset = prev_offset->second;
		}
	} else {
		reservation.offset = global_offset++;
	}

	return reservation;
}

unique_ptr<GlobalFileState> PartitionedCopy::CreatePartitionFileStateFromReservation(const vector<Value> &values,
                                                                                     idx_t offset) {
	// The reservation/eviction decision has already been made under active_writes_lock. This section only
	// serializes global file bookkeeping: directory tracking, filename registration, and writer initialization.
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
	// Initialize write
	return copy_gstate.CreateFileStateLocked(full_path, values);
}

void PartitionedCopy::EnsureFreshPartitionFileForSortedRun(PartitionWriteInfo &write_info,
                                                           const vector<Value> &values) {
	if (op.order_columns.empty()) {
		return;
	}
	EnsureFreshPartitionFile(write_info, values, FileCreationReason::SORTED_RUN_BOUNDARY);
}

void PartitionedCopy::EnsureFreshPartitionFileForRotation(PartitionWriteInfo &write_info, const vector<Value> &values) {
	if (!op.Rotate()) {
		return;
	}
	EnsureFreshPartitionFile(write_info, values, FileCreationReason::ROTATION);
}

void PartitionedCopy::EnsureFreshPartitionFile(PartitionWriteInfo &write_info, const vector<Value> &values,
                                               FileCreationReason reason) {
	D_ASSERT(reason == FileCreationReason::SORTED_RUN_BOUNDARY || reason == FileCreationReason::ROTATION);
	D_ASSERT(RequiresSerializedPartitionWrites());

	optional_ptr<GlobalFileState> old_file_state_ptr;
	{
		annotated_lock_guard<annotated_mutex> global_guard(copy_gstate.lock);
		if (!write_info.file_state) {
			return;
		}
		old_file_state_ptr = write_info.file_state.get();
		annotated_lock_guard<annotated_mutex> file_guard(old_file_state_ptr->lock);
		if (reason == FileCreationReason::SORTED_RUN_BOUNDARY && old_file_state_ptr->num_batches == 0) {
			return;
		}
		if (reason == FileCreationReason::ROTATION && !PhysicalCopyRotateNow(op, *old_file_state_ptr)) {
			return;
		}
	}

	auto new_file_state = CreatePartitionFileState(values, reason);

	unique_ptr<GlobalFileState> old_file_state;
	{
		annotated_lock_guard<annotated_mutex> global_guard(copy_gstate.lock);
		D_ASSERT(write_info.file_state);
		D_ASSERT(RefersToSameObject(*old_file_state_ptr.get(), *write_info.file_state));
		annotated_lock_guard<annotated_mutex> file_guard(write_info.file_state->lock);
		if (reason == FileCreationReason::SORTED_RUN_BOUNDARY) {
			D_ASSERT(write_info.file_state->num_batches > 0);
		} else {
			D_ASSERT(PhysicalCopyRotateNow(op, *write_info.file_state));
		}

		old_file_state = std::move(write_info.file_state);
		write_info.file_state = std::move(new_file_state);
	}

	copy_gstate.FinalizeFileState(std::move(old_file_state));
}

void PartitionedCopy::FinalizeActiveWrites() {
	vector<unique_ptr<GlobalFileState>> files_to_finalize;
	{
		annotated_lock_guard<annotated_mutex> aw_guard(active_writes_lock);
		for (auto &entry : active_writes) {
			files_to_finalize.push_back(std::move(entry.second->file_state));
		}
		active_writes.clear();
	}
	FinalizeFileStates(std::move(files_to_finalize));
}

void PartitionedCopy::FinalizeFileStates(vector<unique_ptr<GlobalFileState>> files_to_finalize) {
	for (auto &file_state : files_to_finalize) {
		if (file_state) {
			copy_gstate.FinalizeFileState(std::move(file_state));
		}
	}
}

string PartitionedCopy::GetOrCreateDirectory(string path, const vector<Value> &values) {
	auto &fs = FileSystem::GetFileSystem(context);
	copy_gstate.CreateDir(path);
	if (op.hive_file_pattern) {
		for (idx_t i = 0; i < op.partition_columns.size(); i++) {
			const auto &partition_col_name = op.names[op.partition_columns[i]];
			const auto &partition_value = values[i];
			string p_dir;
			p_dir += HivePartitioning::Escape(partition_col_name.GetIdentifierName());
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
// Copy Global State Implementation
//===--------------------------------------------------------------------===//
CopyToFileGlobalState::CopyToFileGlobalState(const PhysicalCopyToFile &op_p, ClientContext &context_p)
    : op(op_p), context(context_p), initialized(false), finalized(false), prepare_global_state(nullptr),
      create_file_state_fun([&]() DUCKDB_EXCLUDES(lock) { return CreateFileState(); }), rows_copied(0),
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
	global_state = CreateFileStateLocked(op.file_path);
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

unique_ptr<GlobalFileState>
CopyToFileGlobalState::CreateFileStateLocked(string output_path, optional_ptr<const vector<Value>> partition_values) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (output_path.empty()) {
		output_path = op.filename_pattern.CreateFilename(fs, op.file_path, op.file_extension, last_file_offset++);
	}
	created_files.push_back(output_path);

	optional_ptr<CopyToFileInfo> written_file_info;
	if (op.return_type != CopyFunctionReturnType::CHANGED_ROWS) {
		written_file_info = AddFile(output_path);
	}

	auto data = op.function.copy_to_initialize_global(context, *op.bind_data, output_path);
	if (written_file_info) {
		op.function.copy_to_get_written_statistics(context, *op.bind_data, *data, *written_file_info->file_stats);

		if (!op.partition_columns.empty()) {
			D_ASSERT(partition_values);
			vector<Value> partition_keys;
			vector<Value> partition_values_as_varchar;
			for (idx_t i = 0; i < op.partition_columns.size(); i++) {
				const auto &partition_col_name = op.names[op.partition_columns[i]];
				const auto &partition_value = (*partition_values)[i];
				partition_keys.emplace_back(partition_col_name);
				partition_values_as_varchar.push_back(partition_value.DefaultCastAs(LogicalType::VARCHAR));
			}
			written_file_info->partition_keys =
			    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(partition_keys),
			               std::move(partition_values_as_varchar));
		}
	}

	if (op.function.initialize_operator) {
		op.function.initialize_operator(*data, op);
	}

	auto res = make_uniq<GlobalFileState>(std::move(data), output_path);
	if (!prepare_global_state.load(std::memory_order_acquire)) {
		prepare_global_state.store(res, std::memory_order_release);
	}
	return res;
}

unique_ptr<GlobalFileState> CopyToFileGlobalState::CreateFileState(string output_path,
                                                                   optional_ptr<const vector<Value>> partition_values) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return CreateFileStateLocked(std::move(output_path), partition_values);
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

unique_ptr<GlobalFileState> CopyToFileGlobalState::FinalizeFileStateLocked(unique_ptr<GlobalFileState> file_state) {
	auto prepare_state = prepare_global_state.load(std::memory_order_acquire);
	if (prepare_state && RefersToSameObject(*prepare_state.get(), *file_state)) {
		prepare_global_state_owned = std::move(file_state);
		return nullptr;
	}
	return file_state;
}

void CopyToFileGlobalState::FinalizeFileState(unique_ptr<GlobalFileState> file_state) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		file_state = FinalizeFileStateLocked(std::move(file_state));
	}
	if (file_state) {
		op.function.copy_to_finalize(context, *op.bind_data, *file_state->data);
	}
}

unique_ptr<GlobalFileState> CopyToFileGlobalState::TryFinalizeOwnedFileStateLocked() {
	if (prepare_global_state_owned) {
		return std::move(prepare_global_state_owned);
	}
	return nullptr;
}

void CopyToFileGlobalState::TryFinalizeOwnedFileState() {
	unique_ptr<GlobalFileState> file_state;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		file_state = TryFinalizeOwnedFileStateLocked();
	}
	if (file_state) {
		op.function.copy_to_finalize(context, *op.bind_data, *file_state->data);
	}
}

//===--------------------------------------------------------------------===//
// Copy Local State Implementation
//===--------------------------------------------------------------------===//
CopyToFileLocalState::CopyToFileLocalState(const PhysicalCopyToFile &op_p, ExecutionContext &context_p,
                                           CopyToFileGlobalState &gstate_p)
    : op(op_p), context(context_p), gstate(gstate_p) {
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
	result["FORMAT"] = StringUtil::Upper(function.name.GetIdentifierName());
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

void PhysicalCopyToFile::ReturnStatistics(DataChunk &chunk, CopyToFileInfo &info) {
	auto &file_stats = *info.file_stats;

	// filename VARCHAR
	chunk.data[0].Append(Value(info.file_path));
	// count BIGINT
	chunk.data[1].Append(Value::UBIGINT(file_stats.row_count));
	// file size bytes BIGINT
	chunk.data[2].Append(Value::UBIGINT(file_stats.file_size_bytes));
	// footer size bytes BIGINT
	chunk.data[3].Append(file_stats.footer_size_bytes);
	// column statistics map(varchar, map(varchar, varchar))
	auto column_stats = CreateColumnStatistics(file_stats.column_statistics);
	auto map_val_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	chunk.data[4].Append(
	    Value::MAP(LogicalType::VARCHAR, map_val_type, std::move(column_stats.keys), std::move(column_stats.values)));

	// partition_keys map(varchar, varchar)
	chunk.data[5].Append(info.partition_keys);
}

bool PhysicalCopyToFile::Rotate() const {
	return file_size_bytes.IsValid() || batches_per_file.IsValid();
}

static bool PhysicalCopyRotateNow(const PhysicalCopyToFile &op, GlobalFileState &global_state)
    DUCKDB_REQUIRES(global_state.lock) {
	if (op.file_size_bytes.IsValid()) {
		return op.function.file_size_bytes(*global_state.data) >= op.file_size_bytes.GetIndex();
	}
	if (op.batches_per_file.IsValid()) {
		return global_state.num_batches >= op.batches_per_file.GetIndex();
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
		if (!partition_output && !per_thread_output && Rotate() && write_empty_file) {
			annotated_lock_guard<annotated_mutex> guard(state->lock);
			state->global_state = state->CreateFileStateLocked();
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
	return make_uniq<CopyToFileLocalState>(*this, context, gstate);
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
		PrepareAndFlushBatch(context.client, gstate, file_state_ptr, gstate.create_file_state_fun,
		                     std::move(lstate.batch));
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
		gstate.partitioned_copy->Combine(context, *lstate.partitioned_copy_local_state, input.interrupt_state,
		                                 PartitionedCopyCombineType::DURING_PIPELINE_COMBINE);
		return SinkCombineResultType::FINISHED;
	}

	if (per_thread_output) {
		if (lstate.batch) {
			PrepareAndFlushBatch(context.client, gstate, lstate.global_file_state, gstate.create_file_state_fun,
			                     std::move(lstate.batch));
		}
		if (lstate.global_file_state) {
			gstate.FinalizeFileState(std::move(lstate.global_file_state));
		}
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
	PrepareAndFlushBatch(context.client, gstate, file_state_ptr, gstate.create_file_state_fun, std::move(batch));

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CopyToFileGlobalState>();
	gstate.finalized = true;

	if (partition_output) {
		gstate.partitioned_copy->Finalize(pipeline, event, input.interrupt_state);
		return SinkFinalizeType::READY;
	}

	{
		annotated_lock_guard<annotated_mutex> guard(gstate.last_batch_lock);
		if (gstate.last_batch) {
			unique_ptr<LocalFunctionData> lstate;
			PrepareAndFlushBatch(context, gstate, gstate.global_state, gstate.create_file_state_fun,
			                     std::move(gstate.last_batch));
		}
	}

	if (per_thread_output) {
		// already happened in combine
		if (NumericCast<int64_t>(gstate.rows_copied.load()) == 0 && sink_state != nullptr) {
			// no rows from source, write schema to file
			annotated_lock_guard<annotated_mutex> guard(gstate.lock);
			gstate.global_state = gstate.CreateFileStateLocked();
		}
	}

	if (gstate.global_state) {
		gstate.FinalizeFileState(std::move(gstate.global_state));

		if (use_tmp_file) {
			D_ASSERT(!per_thread_output);
			D_ASSERT(!partition_output);
			D_ASSERT(!file_size_bytes.IsValid());
			D_ASSERT(!Rotate());
			MoveTmpFile(context, file_path);
		}
	}

	gstate.TryFinalizeOwnedFileState();

	return SinkFinalizeType::READY;
}

void PhysicalCopyToFile::PrepareAndFlushBatch(ClientContext &context, GlobalSinkState &gstate_p,
                                              unique_ptr<GlobalFileState> &file_state_ptr,
                                              const std::function<unique_ptr<GlobalFileState>()> &create_file_state_fun,
                                              unique_ptr<ColumnDataCollection> batch) const {
	auto [batch_analyzer, prepared_batch] =
	    PrepareBatch(context, gstate_p, file_state_ptr, create_file_state_fun, std::move(batch));
	FlushBatch(context, gstate_p, file_state_ptr, create_file_state_fun, batch_analyzer, std::move(prepared_batch));
}

//===--------------------------------------------------------------------===//
// Legacy
//===--------------------------------------------------------------------===//
struct LegacyCopyPreparedBatch : public PreparedBatchData {
	explicit LegacyCopyPreparedBatch(unique_ptr<ColumnDataCollection> collection_p)
	    : collection(std::move(collection_p)) {
	}

	unique_ptr<ColumnDataCollection> collection;
};

static bool UsesLegacyCopyBatchAPI(const CopyFunction &function) {
	if (!function.prepare_batch && !function.flush_batch) {
		return true;
	}
	if (!function.prepare_batch || !function.flush_batch) {
		throw InternalException("Copy function must implement both prepare_batch and flush_batch");
	}
	return false;
}

static unique_ptr<PreparedBatchData> PrepareLegacyCopyBatch(unique_ptr<ColumnDataCollection> batch) {
	return make_uniq<LegacyCopyPreparedBatch>(std::move(batch));
}

static void FlushLegacyCopyBatch(ClientContext &context, const CopyFunction &function, FunctionData &bind_data,
                                 GlobalFunctionData &gstate, PreparedBatchData &prepared_batch) {
	if (!function.copy_to_initialize_local || !function.copy_to_sink || !function.copy_to_combine) {
		throw InternalException("Legacy copy function is missing required sink/combine callbacks");
	}

	auto &legacy_batch = prepared_batch.Cast<LegacyCopyPreparedBatch>();
	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto local_state = function.copy_to_initialize_local(execution_context, bind_data);
	for (auto &chunk : legacy_batch.collection->Chunks()) {
		function.copy_to_sink(execution_context, bind_data, gstate, *local_state, chunk);
	}
	function.copy_to_combine(execution_context, bind_data, gstate, *local_state);
}

//===--------------------------------------------------------------------===//
// Prepare/Flush Batch
//===--------------------------------------------------------------------===//
static void EnsureFileState(CopyToFileGlobalState &gstate, unique_ptr<GlobalFileState> &file_state_ptr,
                            const std::function<unique_ptr<GlobalFileState>()> &create_file_state_fun) {
	auto file_state_key = &file_state_ptr;
	while (true) {
		bool create_file_state = false;
		{
			annotated_lock_guard<annotated_mutex> guard(gstate.lock);
			if (file_state_ptr) {
				return;
			}
			if (gstate.creating_file_states.insert(file_state_key).second) {
				create_file_state = true;
			}
		}

		if (!create_file_state) {
			TaskScheduler::YieldThread();
			continue;
		}

		unique_ptr<GlobalFileState> new_file_state;
		try {
			new_file_state = create_file_state_fun();
		} catch (...) {
			annotated_lock_guard<annotated_mutex> guard(gstate.lock);
			gstate.creating_file_states.erase(file_state_key);
			throw;
		}

		unique_ptr<GlobalFileState> unused_file_state;
		{
			annotated_lock_guard<annotated_mutex> guard(gstate.lock);
			if (!file_state_ptr) {
				file_state_ptr = std::move(new_file_state);
			} else {
				unused_file_state = std::move(new_file_state);
			}
			gstate.creating_file_states.erase(file_state_key);
		}
		if (unused_file_state) {
			gstate.FinalizeFileState(std::move(unused_file_state));
		}
		return;
	}
}

pair<const CopyFunctionBatchAnalyzer, unique_ptr<PreparedBatchData>>
PhysicalCopyToFile::PrepareBatch(ClientContext &context, GlobalSinkState &gstate_p,
                                 unique_ptr<GlobalFileState> &file_state_ptr,
                                 const std::function<unique_ptr<GlobalFileState>()> &create_file_state_fun,
                                 unique_ptr<ColumnDataCollection> batch) const {
	auto &gstate = gstate_p.Cast<CopyToFileGlobalState>();
	const CopyFunctionBatchAnalyzer batch_analyzer(*batch, batch_size, batch_size_bytes);

	if (UsesLegacyCopyBatchAPI(function)) {
		return {batch_analyzer, PrepareLegacyCopyBatch(std::move(batch))};
	}

	// Ensure we have a global state for prepares
	auto prepare_global_state = gstate.prepare_global_state.load(std::memory_order_acquire);
	if (!prepare_global_state) {
		D_ASSERT(!file_state_ptr);
		EnsureFileState(gstate, file_state_ptr, create_file_state_fun);
		prepare_global_state = gstate.prepare_global_state.load(std::memory_order_acquire);
		D_ASSERT(prepare_global_state);
	}

	// Prepare the batch
	return {batch_analyzer, function.prepare_batch(context, *bind_data, *prepare_global_state->data, std::move(batch))};
}

void PhysicalCopyToFile::FlushBatch(ClientContext &context, GlobalSinkState &gstate_p,
                                    unique_ptr<GlobalFileState> &file_state_ptr,
                                    const std::function<unique_ptr<GlobalFileState>()> &create_file_state_fun,
                                    const CopyFunctionBatchAnalyzer &batch_analyzer,
                                    unique_ptr<PreparedBatchData> prepared_batch) const {
	auto &gstate = gstate_p.Cast<CopyToFileGlobalState>();

	while (true) {
		EnsureFileState(gstate, file_state_ptr, create_file_state_fun);

		// Decide which file to flush to
		annotated_unique_lock<annotated_mutex> global_guard(gstate.lock);
		if (!file_state_ptr) {
			global_guard.unlock();
			TaskScheduler::YieldThread();
			continue;
		}

		annotated_unique_lock<annotated_mutex> file_guard(file_state_ptr->lock);
		if (PhysicalCopyRotateNow(*this, *file_state_ptr)) {
			// Global state must be rotated. Move to local scope, create an new one, and immediately release global lock
			auto owned_file_state = std::move(file_state_ptr);
			file_guard.unlock();
			global_guard.unlock();

			EnsureFileState(gstate, file_state_ptr, create_file_state_fun);

			// Finalize this file!
			gstate.FinalizeFileState(std::move(owned_file_state));
		} else {
			global_guard.unlock();
			file_state_ptr->num_batches++;

			DUCKDB_LOG(context, PhysicalOperatorLogType, *this, "PhysicalCopyToFile", "FlushBatch",
			           {{"file", file_state_ptr->path},
			            {"rows", to_string(batch_analyzer.current_batch_size)},
			            {"size", to_string(batch_analyzer.current_batch_size_bytes)},
			            {"reason", EnumUtil::ToString(batch_analyzer.ToReason())}});
			if (UsesLegacyCopyBatchAPI(function)) {
				FlushLegacyCopyBatch(context, function, *bind_data, *file_state_ptr->data, *prepared_batch);
			} else {
				function.flush_batch(context, *bind_data, *file_state_ptr->data, *prepared_batch);
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
			ReturnStatistics(chunk, file_entry);
		}
		source_state.offset += count;
		return source_state.offset < gstate.written_files.size() ? SourceResultType::HAVE_MORE_OUTPUT
		                                                         : SourceResultType::FINISHED;
	}

	switch (return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(gstate.rows_copied.load())));
		break;
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST: {
		chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(gstate.rows_copied.load())));
		vector<Value> file_name_list;
		for (auto &file_info : gstate.written_files) {
			if (use_tmp_file) {
				file_name_list.emplace_back(GetNonTmpFile(context.client, file_info->file_path));
			} else {
				file_name_list.emplace_back(file_info->file_path);
			}
		}
		chunk.data[1].Append(Value::LIST(LogicalType::VARCHAR, std::move(file_name_list)));
		break;
	}
	default:
		throw NotImplementedException("Unknown CopyFunctionReturnType");
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
