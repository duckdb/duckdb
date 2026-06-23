#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/sorting/sort_strategy.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/function/window/window_collection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/main/settings.hpp"
#include "fmt/format.h"

#include <algorithm>
#include <condition_variable>
#include <exception>
#include <functional>
#include <type_traits>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Declarations
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// Utility Declarations
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

//===--------------------------------------------------------------------===//
// Copy File State Types
//===--------------------------------------------------------------------===//
struct GlobalFileState {
public:
	explicit GlobalFileState(unique_ptr<GlobalFunctionData> data_p, const string &path_p)
	    : data(std::move(data_p)), path(path_p), num_batches(0) {
	}

public:
	annotated_mutex lock;
	unique_ptr<GlobalFunctionData> data;
	const string path;
	idx_t num_batches DUCKDB_GUARDED_BY(lock);
};

//===--------------------------------------------------------------------===//
// Copy File State Declarations
//===--------------------------------------------------------------------===//
struct PendingFileState {
	string output_path;
	optional_ptr<CopyToFileInfo> written_file_info;
};

struct PartitionDirectory {
	string path;
	vector<string> directories;
};

enum class CopyDirectoryState : uint8_t { PENDING, COMPLETE, FAILED };

class CopyDirectoryManager {
public:
	void EnsureDirectory(FileSystem &fs, const string &dir_path);

private:
	struct DirectoryEntry {
		CopyDirectoryState state = CopyDirectoryState::PENDING;
		std::exception_ptr error;
	};

private:
	mutex lock;
	std::condition_variable condition;
	unordered_map<string, DirectoryEntry> directories;
};

class CopyOutputFileRegistry {
public:
	explicit CopyOutputFileRegistry(const PhysicalCopyToFile &op_p) : op(op_p) {
	}

public:
	//! The registry is guarded by CopyToFileGlobalState::lock.
	PendingFileState ReserveFile(string output_path, optional_ptr<const vector<Value>> partition_values);
	void PublishCreatedPath(PendingFileState &pending_file_state, string output_path);

	idx_t WrittenFileCount() const {
		return written_files.size();
	}

	CopyToFileInfo &GetWrittenFile(idx_t file_idx) {
		return *written_files[file_idx];
	}

	const vector<unique_ptr<CopyToFileInfo>> &GetWrittenFiles() const {
		return written_files;
	}

	bool HasCreatedFiles() const {
		return !created_files.empty();
	}

	const vector<string> &GetCreatedFiles() const {
		return created_files;
	}

private:
	optional_ptr<CopyToFileInfo> AddFile(const string &file_name);

private:
	const PhysicalCopyToFile &op;
	vector<string> created_files;
	vector<unique_ptr<CopyToFileInfo>> written_files;
};

//===--------------------------------------------------------------------===//
// Copy File Lifecycle Declarations
//===--------------------------------------------------------------------===//
enum class CopyFileLifecycleWaitMode : uint8_t { INTERRUPTIBLE, DRAIN };

class CopyFileLifecycleJob {
public:
	bool IsFinished() const {
		return finished.load(std::memory_order_acquire);
	}

	void Complete() {
		finished.store(true, std::memory_order_release);
	}

	void CompleteException(const std::exception_ptr &error_p) {
		error = error_p;
		Complete();
	}

	void Rethrow() const {
		if (error) {
			std::rethrow_exception(error);
		}
	}

private:
	atomic<bool> finished {false};
	std::exception_ptr error;
};

class FileStateOpenJob : public CopyFileLifecycleJob {
public:
	void Complete(unique_ptr<GlobalFileState> file_state_p) {
		file_state = std::move(file_state_p);
		CopyFileLifecycleJob::Complete();
	}

	GlobalFileState &GetFileState() const {
		D_ASSERT(IsFinished());
		Rethrow();
		D_ASSERT(file_state);
		return *file_state;
	}

	unique_ptr<GlobalFileState> TakeFileState() {
		D_ASSERT(IsFinished());
		Rethrow();
		return std::move(file_state);
	}

private:
	unique_ptr<GlobalFileState> file_state;
};

struct PendingFileStateOpen {
	PendingFileState pending_file_state;
	shared_ptr<FileStateOpenJob> open_job;

	explicit operator bool() const {
		return open_job.get();
	}
};

class CopyToFileGlobalState;

struct PartitionFileOpenRequest {
	PartitionFileOpenRequest(PendingFileStateOpen pending_file_state_open, PartitionDirectory directory_p,
	                         idx_t offset_p)
	    : pending_file_state(std::move(pending_file_state_open.pending_file_state)),
	      open_job(std::move(pending_file_state_open.open_job)), directory(std::move(directory_p)), offset(offset_p) {
	}

	void Run(CopyToFileGlobalState &copy_gstate);

	PendingFileState pending_file_state;
	shared_ptr<FileStateOpenJob> open_job;
	PartitionDirectory directory;
	idx_t offset;
};

struct FileStateHandle {
public:
	FileStateHandle() = default;
	FileStateHandle(FileStateHandle &&) = default;
	FileStateHandle &operator=(FileStateHandle &&) = default;
	FileStateHandle(const FileStateHandle &) = delete;
	FileStateHandle &operator=(const FileStateHandle &) = delete;

public:
	bool HasFileState() const {
		return open_job.get();
	}

	bool IsReady() const {
		return open_job && open_job->IsFinished();
	}

	GlobalFileState &GetFileState() const {
		D_ASSERT(open_job);
		return open_job->GetFileState();
	}

	optional_ptr<GlobalFileState> GetFileStatePtr() const {
		if (!IsReady()) {
			return nullptr;
		}
		return open_job->GetFileState();
	}

	unique_ptr<GlobalFileState> TakeFileState() {
		if (!open_job) {
			return nullptr;
		}
		auto result = open_job->TakeFileState();
		open_job.reset();
		return result;
	}

	explicit operator bool() const {
		return HasFileState();
	}

public:
	shared_ptr<FileStateOpenJob> open_job;
};

struct PartitionFileRequest {
	PartitionFileRequest(PartitionFileOpenRequest open_request_p, vector<FileStateHandle> files_to_finalize_p)
	    : open_request(std::move(open_request_p)), files_to_finalize(std::move(files_to_finalize_p)) {
	}

	shared_ptr<FileStateOpenJob> OpenJob() const {
		return open_request.open_job;
	}

	PartitionFileOpenRequest open_request;
	vector<FileStateHandle> files_to_finalize;
};

template <class FUNC>
class CopyFileLifecycleTask;

class CopyFileLifecycleExecutor {
	static constexpr idx_t MIN_PENDING_TASKS = 4096;

public:
	explicit CopyFileLifecycleExecutor(ClientContext &context_p)
	    : context(context_p), executor(context_p, TaskSchedulerType::ASYNC) {
		auto &scheduler = TaskScheduler::GetScheduler(context);
		async_threads = NumericCast<idx_t>(scheduler.NumberOfAsyncThreads());
		auto regular_threads = NumericCast<idx_t>(scheduler.NumberOfThreads());
		max_pending_tasks = MaxValue<idx_t>(MIN_PENDING_TASKS, (async_threads + regular_threads) * 4);
	}

public:
	template <class FUNC>
	void Schedule(shared_ptr<CopyFileLifecycleJob> job, CopyFileLifecycleWaitMode mode, FUNC &&task);
	void WaitForJob(CopyFileLifecycleJob &job, CopyFileLifecycleWaitMode mode);
	void WaitAll(CopyFileLifecycleWaitMode mode);
	void WorkOnTaskOrYield();
	void FinishTask();
	void PushError(const std::exception_ptr &error);

private:
	bool WorkOnTask(bool throw_error = true);
	void WaitForTaskSlot(CopyFileLifecycleWaitMode mode);
	void ThrowError();

private:
	ClientContext &context;
	TaskExecutor executor;
	idx_t async_threads;
	idx_t max_pending_tasks;
	atomic<idx_t> pending_tasks {0};
	mutex error_lock;
	std::exception_ptr error;
};

class CopyFileLifecycleTaskFinishGuard {
public:
	CopyFileLifecycleTaskFinishGuard(TaskExecutor &executor_p, CopyFileLifecycleExecutor &lifecycle_p)
	    : executor(executor_p), lifecycle(lifecycle_p) {
	}

	~CopyFileLifecycleTaskFinishGuard() {
		Finish();
	}

	void Finish() {
		if (!finished) {
			lifecycle.FinishTask();
			executor.FinishTask();
			finished = true;
		}
	}

private:
	TaskExecutor &executor;
	CopyFileLifecycleExecutor &lifecycle;
	bool finished = false;
};

template <class FUNC>
class CopyFileLifecycleTask : public Task {
public:
	CopyFileLifecycleTask(TaskExecutor &executor_p, CopyFileLifecycleExecutor &lifecycle_p,
	                      shared_ptr<CopyFileLifecycleJob> job_p, FUNC task_p)
	    : executor(executor_p), lifecycle(lifecycle_p), job(std::move(job_p)), task(std::move(task_p)) {
	}

public:
	TaskExecutionResult Execute(TaskExecutionMode mode) override {
		CopyFileLifecycleTaskFinishGuard finish_guard(executor, lifecycle);
		try {
			task();
			if (!job->IsFinished()) {
				job->Complete();
			}
		} catch (...) {
			auto error = std::current_exception();
			job->CompleteException(error);
			lifecycle.PushError(error);
		}
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "CopyFileLifecycleTask";
	}

private:
	TaskExecutor &executor;
	CopyFileLifecycleExecutor &lifecycle;
	shared_ptr<CopyFileLifecycleJob> job;
	FUNC task;
};

template <class FUNC>
void CopyFileLifecycleExecutor::Schedule(shared_ptr<CopyFileLifecycleJob> job, CopyFileLifecycleWaitMode mode,
                                         FUNC &&task) {
	WaitForTaskSlot(mode);
	auto job_ref = job;
	++pending_tasks;
	try {
		using TaskType = CopyFileLifecycleTask<typename std::decay<FUNC>::type>;
		executor.ScheduleTask(make_uniq<TaskType>(executor, *this, std::move(job), std::forward<FUNC>(task)));
	} catch (...) {
		--pending_tasks;
		throw;
	}
	if (async_threads == 0) {
		WaitForJob(*job_ref, mode);
	}
}

//===--------------------------------------------------------------------===//
// Copy State Declarations
//===--------------------------------------------------------------------===//
class PartitionedCopy;

class CopyToFileGlobalState : public GlobalSinkState {
public:
	CopyToFileGlobalState(const PhysicalCopyToFile &op_p, ClientContext &context_p);
	~CopyToFileGlobalState() override;

public:
	void Initialize() DUCKDB_EXCLUDES(lock);

	PendingFileState PrepareFileStateLocked(string output_path = string(),
	                                        optional_ptr<const vector<Value>> partition_values = nullptr)
	    DUCKDB_REQUIRES(lock);
	PendingFileStateOpen CreateFileStateOpenLocked(FileStateHandle &file_state, string output_path = string(),
	                                               optional_ptr<const vector<Value>> partition_values = nullptr)
	    DUCKDB_REQUIRES(lock);
	PendingFileStateOpen CreatePartitionFileStateOpenLocked(FileStateHandle &file_state, string output_path,
	                                                        optional_ptr<const vector<Value>> partition_values)
	    DUCKDB_REQUIRES(lock);
	unique_ptr<GlobalFileState> InitializeFileState(PendingFileState pending_file_state) DUCKDB_EXCLUDES(lock);
	void RegisterPrepareGlobalStateLocked(GlobalFileState &file_state) DUCKDB_REQUIRES(lock);
	void ScheduleOutputDirectorySetup() DUCKDB_EXCLUDES(lock);
	void EnsureOutputDirectoryReady() DUCKDB_EXCLUDES(lock);
	void ScheduleFileStateOpen(PendingFileStateOpen pending_file_state_open) DUCKDB_EXCLUDES(lock);
	void SchedulePartitionFileStateOpen(PartitionFileOpenRequest request) DUCKDB_EXCLUDES(lock);
	void RequestFileState(FileStateHandle &file_state, string output_path = string(),
	                      optional_ptr<const vector<Value>> partition_values = nullptr) DUCKDB_EXCLUDES(lock);
	GlobalFileState &EnsureFileStateReady(FileStateHandle &file_state,
	                                      const std::function<void(FileStateHandle &)> &create_file_state_fun)
	    DUCKDB_EXCLUDES(lock);
	FileStateHandle FinalizeFileStateLocked(FileStateHandle file_state) DUCKDB_REQUIRES(lock);
	void FinalizeFileState(FileStateHandle file_state) DUCKDB_EXCLUDES(lock);

	FileStateHandle TryFinalizeOwnedFileStateLocked() DUCKDB_REQUIRES(lock);
	void TryFinalizeOwnedFileState() DUCKDB_EXCLUDES(lock);
	void WaitForLifecycleTasks() DUCKDB_EXCLUDES(lock);

private:
	void PrepareOutputDirectory() DUCKDB_EXCLUDES(lock);
	void EnsureDirectory(const string &dir_path) DUCKDB_EXCLUDES(lock);
	void RegisterPendingFileStatePathLocked(PendingFileState &pending_file_state, string output_path)
	    DUCKDB_REQUIRES(lock);

	friend struct PartitionFileOpenRequest;

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
	FileStateHandle prepare_global_state_owned DUCKDB_GUARDED_BY(lock);

	//! The (current) global state
	FileStateHandle global_state;
	//! Lambda to create a new global file state
	const std::function<void(FileStateHandle &)> create_file_state_fun;
	//! Asynchronously prepares the root output directory for directory-style COPY outputs.
	shared_ptr<CopyFileLifecycleJob> output_directory_job;
	CopyFileLifecycleExecutor lifecycle_executor;
	CopyDirectoryManager directory_manager;
	CopyOutputFileRegistry output_files;

	//! The final batch
	mutable annotated_mutex last_batch_lock;
	unique_ptr<ColumnDataCollection> last_batch DUCKDB_GUARDED_BY(last_batch_lock);

	//! Partitioning state
	unique_ptr<PartitionedCopy> partitioned_copy;

	//! Counters
	atomic<idx_t> rows_copied;
	atomic<idx_t> last_file_offset;
};

//===--------------------------------------------------------------------===//
// Copy Local State Declarations
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
	FileStateHandle global_file_state;
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
// Partitioned Copy Type Declarations
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
	FileStateHandle file_state;
	idx_t active_writes = 0;
};

struct PartitionFileStateReservation {
	vector<FileStateHandle> files_to_finalize;
	idx_t offset = 0;
};

//===--------------------------------------------------------------------===//
// Partition Write Manager Declarations
//===--------------------------------------------------------------------===//
class PartitionWriteManager;

class PartitionWriteLease {
public:
	PartitionWriteLease() = default;
	PartitionWriteLease(PartitionWriteManager &manager_p, PartitionWriteInfo &write_info_p);
	PartitionWriteLease(PartitionWriteLease &&other) noexcept;
	PartitionWriteLease &operator=(PartitionWriteLease &&other) noexcept;
	PartitionWriteLease(const PartitionWriteLease &) = delete;
	PartitionWriteLease &operator=(const PartitionWriteLease &) = delete;
	~PartitionWriteLease();

public:
	explicit operator bool() const {
		return write_info.get();
	}

	PartitionWriteInfo &Get() const {
		D_ASSERT(write_info);
		return *write_info.get_mutable();
	}

	PartitionWriteInfo &operator*() const {
		return Get();
	}

	PartitionWriteInfo *operator->() const {
		return &Get();
	}

	void Reset();

private:
	optional_ptr<PartitionWriteManager> manager;
	optional_ptr<PartitionWriteInfo> write_info;
};

class PartitionWriteManager {
public:
	class ReservationLock {
	public:
		ReservationLock(ReservationLock &&) noexcept = default;
		ReservationLock &operator=(ReservationLock &&) noexcept = default;
		ReservationLock(const ReservationLock &) = delete;
		ReservationLock &operator=(const ReservationLock &) = delete;

	private:
		explicit ReservationLock(annotated_mutex &lock_p) : guard(lock_p) {
		}

	private:
		annotated_unique_lock<annotated_mutex> guard;
		friend class PartitionWriteManager;
	};

public:
	PartitionWriteManager(const PhysicalCopyToFile &op_p, ClientContext &context_p) : op(op_p), context(context_p) {
	}

public:
	PartitionWriteLease Acquire(const vector<Value> &values) DUCKDB_EXCLUDES(lock);
	ReservationLock LockForReservation() DUCKDB_EXCLUDES(lock);
	PartitionFileStateReservation ReserveFileState(ReservationLock &reservation_lock, const vector<Value> &values,
	                                               FileCreationReason reason) DUCKDB_NO_THREAD_SAFETY_ANALYSIS;
	vector<FileStateHandle> TakeOpenFileStates() DUCKDB_EXCLUDES(lock);

private:
	void Release(PartitionWriteInfo &write_info) DUCKDB_EXCLUDES(lock);

private:
	const PhysicalCopyToFile &op;
	ClientContext &context;
	mutable annotated_mutex lock;
	vector_of_value_map_t<unique_ptr<PartitionWriteInfo>> active_writes DUCKDB_GUARDED_BY(lock);
	vector_of_value_map_t<idx_t> previous_partitions DUCKDB_GUARDED_BY(lock);
	idx_t global_offset DUCKDB_GUARDED_BY(lock) = 0;

	friend class PartitionWriteLease;
};

//===--------------------------------------------------------------------===//
// Partitioned Copy Batch State Declarations
//===--------------------------------------------------------------------===//
enum class PartitionedCopyBatchMode : uint8_t { BUFFERING, PREPARING, DELAYED, PREPARED };
enum class PartitionedCopyBatchActionType : uint8_t { STORE_COLLECTION, ACQUIRE_AND_PREPARE, PREPARE_WITH_WRITE_INFO };
enum class PartitionedCopyPrepareTaskActionType : uint8_t {
	SKIP_PARTITION,
	WAIT_FOR_WRITE_INFO,
	ACQUIRE_WRITE_INFO,
	PREPARE_BATCH
};
enum class PartitionedCopyPrepareActionType : uint8_t { ACQUIRE_WRITE_INFO, PREPARE_BATCH };
enum class PartitionedCopyFlushActionType : uint8_t { DELAYED_COLLECTIONS, PREPARED_BATCHES };

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

struct PartitionedCopyBatchAction {
	PartitionedCopyBatchActionType type = PartitionedCopyBatchActionType::STORE_COLLECTION;
	vector<Value> values;
	optional_ptr<PartitionWriteInfo> write_info;
};

struct PartitionedCopyPrepareTaskAction {
	PartitionedCopyPrepareTaskActionType type = PartitionedCopyPrepareTaskActionType::SKIP_PARTITION;
	idx_t batch_idx = DConstants::INVALID_INDEX;
};

struct PartitionedCopyPrepareAction {
	PartitionedCopyPrepareActionType type = PartitionedCopyPrepareActionType::ACQUIRE_WRITE_INFO;
	vector<Value> values;
	optional_ptr<PartitionWriteInfo> write_info;
	PartitionedCopyCollection data;
};

struct PartitionedCopyFlushAction {
	PartitionedCopyFlushActionType type = PartitionedCopyFlushActionType::DELAYED_COLLECTIONS;
	vector<Value> values;
	vector<PartitionedCopyCollection> collections;
	vector<unique_ptr<PartitionedCopyBatch>> batches;
	PartitionWriteLease write_lease;
};

struct PartitionedCopyBatchState {
private:
	void SetValues(vector<Value> values_p) {
		D_ASSERT(values.empty() || values == values_p);
		if (values.empty()) {
			values = std::move(values_p);
		}
	}

	void StartPreparing() {
		D_ASSERT(mode == PartitionedCopyBatchMode::BUFFERING);
		mode = PartitionedCopyBatchMode::PREPARING;
		batches.resize(collections.size());
	}

	bool TryReserveWriteInfo() {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		if (write_lease || write_info_requested) {
			return false;
		}
		write_info_requested = true;
		return true;
	}

	PartitionWriteInfo &SetWriteInfo(PartitionWriteLease write_lease_p) {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		D_ASSERT(!write_lease);
		D_ASSERT(write_lease_p);
		write_lease = std::move(write_lease_p);
		write_info_requested = false;
		batches.resize(collections.size());
		return write_lease.Get();
	}

	void EnsurePreparingBatchSlots() {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		D_ASSERT(write_lease);
		batches.resize(collections.size());
	}

	void MarkDelayed() {
		D_ASSERT(mode == PartitionedCopyBatchMode::BUFFERING);
		mode = PartitionedCopyBatchMode::DELAYED;
	}

	bool NeedsWriteInfo() const {
		return mode == PartitionedCopyBatchMode::PREPARING && !write_lease;
	}

	bool NeedsPrepare(idx_t batch_idx) const {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		D_ASSERT(write_lease);
		D_ASSERT(batch_idx < collections.size());
		return collections[batch_idx].collection && (batch_idx >= batches.size() || !batches[batch_idx]);
	}

	bool HasWriteInfo() const {
		return write_lease.operator bool();
	}

	PartitionWriteInfo &GetWriteInfo() const {
		return write_lease.Get();
	}

	PartitionWriteLease TakeWriteLease() {
		D_ASSERT(write_lease);
		return std::move(write_lease);
	}

public:
	idx_t NextCollectionIndex() const {
		return collections.size();
	}

	const vector<Value> &Values() const {
		return values;
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

	PartitionedCopyBatchAction RegisterBatch(vector<Value> values_p, idx_t flush_threshold,
	                                         bool has_delayed_partition) {
		SetValues(std::move(values_p));
		auto result = PartitionedCopyBatchAction {PartitionedCopyBatchActionType::STORE_COLLECTION, values, nullptr};
		if (mode == PartitionedCopyBatchMode::BUFFERING && count >= flush_threshold && !has_delayed_partition) {
			StartPreparing();
			if (TryReserveWriteInfo()) {
				result.type = PartitionedCopyBatchActionType::ACQUIRE_AND_PREPARE;
				return result;
			}
		}

		if (mode == PartitionedCopyBatchMode::PREPARING && write_lease) {
			result.type = PartitionedCopyBatchActionType::PREPARE_WITH_WRITE_INFO;
			result.write_info = GetWriteInfo();
		}
		return result;
	}

	PartitionWriteInfo &CompleteWriteInfoAcquisition(PartitionWriteLease write_lease_p) {
		D_ASSERT(write_info_requested);
		return SetWriteInfo(std::move(write_lease_p));
	}

	void StoreCollection(idx_t batch_idx, unique_ptr<ColumnDataCollection> collection) {
		D_ASSERT(mode == PartitionedCopyBatchMode::BUFFERING || mode == PartitionedCopyBatchMode::PREPARING);
		D_ASSERT(batch_idx < collections.size());
		collections[batch_idx].collection = std::move(collection);
	}

	idx_t FinalizeBatching(idx_t flush_threshold, bool has_delayed_partition) {
		D_ASSERT(!values.empty());
		D_ASSERT(count > 0);
		if (mode == PartitionedCopyBatchMode::PREPARING) {
			D_ASSERT(HasWriteInfo());
			EnsurePreparingBatchSlots();
			return 0;
		}
		D_ASSERT(mode == PartitionedCopyBatchMode::BUFFERING);
		if (count < flush_threshold || has_delayed_partition) {
			MarkDelayed();
			return count;
		}
		StartPreparing();
		return 0;
	}

	PartitionedCopyPrepareTaskAction SelectPrepareTask(idx_t &prepare_batch_idx) {
		if (mode == PartitionedCopyBatchMode::DELAYED || mode == PartitionedCopyBatchMode::PREPARED) {
			return {PartitionedCopyPrepareTaskActionType::SKIP_PARTITION};
		}
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		if (NeedsWriteInfo()) {
			if (!write_info_requested && TryReserveWriteInfo()) {
				return {PartitionedCopyPrepareTaskActionType::ACQUIRE_WRITE_INFO};
			}
			return {PartitionedCopyPrepareTaskActionType::WAIT_FOR_WRITE_INFO};
		}
		while (prepare_batch_idx < collections.size()) {
			if (NeedsPrepare(prepare_batch_idx)) {
				return {PartitionedCopyPrepareTaskActionType::PREPARE_BATCH, prepare_batch_idx++};
			}
			++prepare_batch_idx;
		}
		return {PartitionedCopyPrepareTaskActionType::SKIP_PARTITION};
	}

	PartitionedCopyPrepareAction BeginPrepareTask(idx_t batch_idx) {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		PartitionedCopyPrepareAction result;
		result.values = values;
		if (batch_idx == DConstants::INVALID_INDEX) {
			D_ASSERT(NeedsWriteInfo());
			D_ASSERT(write_info_requested);
			result.type = PartitionedCopyPrepareActionType::ACQUIRE_WRITE_INFO;
			return result;
		}
		D_ASSERT(HasWriteInfo());
		D_ASSERT(batch_idx < collections.size());
		result.type = PartitionedCopyPrepareActionType::PREPARE_BATCH;
		result.write_info = GetWriteInfo();
		result.data = TakeCollection(batch_idx);
		return result;
	}

private:
	PartitionedCopyCollection TakeCollection(idx_t batch_idx) {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING);
		D_ASSERT(batch_idx < collections.size());
		return std::move(collections[batch_idx]);
	}

public:
	void StorePreparedBatch(idx_t batch_idx, unique_ptr<PartitionedCopyBatch> batch) {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARING || mode == PartitionedCopyBatchMode::PREPARED);
		D_ASSERT(batch_idx < batches.size());
		batches[batch_idx] = std::move(batch);
	}

	void MarkPrepared() {
		if (mode == PartitionedCopyBatchMode::PREPARING) {
			mode = PartitionedCopyBatchMode::PREPARED;
		}
	}

	bool ReadyForFlush() const {
		if (mode == PartitionedCopyBatchMode::DELAYED) {
			return true;
		}
		if (mode != PartitionedCopyBatchMode::PREPARING && mode != PartitionedCopyBatchMode::PREPARED) {
			return false;
		}
		if (!HasWriteInfo() || batches.size() < collections.size()) {
			return false;
		}
		for (idx_t batch_idx = 0; batch_idx < collections.size(); batch_idx++) {
			if (!batches[batch_idx]) {
				return false;
			}
		}
		return true;
	}

	PartitionedCopyFlushAction TakeFlushAction() {
		PartitionedCopyFlushAction result;
		result.values = values;
		if (mode == PartitionedCopyBatchMode::DELAYED) {
			result.type = PartitionedCopyFlushActionType::DELAYED_COLLECTIONS;
			result.collections = TakeDelayedCollections();
			return result;
		}
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARED);
		result.type = PartitionedCopyFlushActionType::PREPARED_BATCHES;
		result.write_lease = TakeWriteLease();
		result.batches = TakePreparedBatches();
		return result;
	}

private:
	vector<PartitionedCopyCollection> TakeDelayedCollections() {
		D_ASSERT(mode == PartitionedCopyBatchMode::DELAYED);
		return std::move(collections);
	}

	vector<unique_ptr<PartitionedCopyBatch>> TakePreparedBatches() {
		D_ASSERT(mode == PartitionedCopyBatchMode::PREPARED);
		return std::move(batches);
	}

private:
	vector<Value> values;
	PartitionWriteLease write_lease;
	vector<PartitionedCopyCollection> collections;
	vector<unique_ptr<PartitionedCopyBatch>> batches;
	idx_t count = 0;
	bool write_info_requested = false;
	PartitionedCopyBatchMode mode = PartitionedCopyBatchMode::BUFFERING;
};

//===--------------------------------------------------------------------===//
// Delayed Partition Buffering Declarations
//===--------------------------------------------------------------------===//
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

//===--------------------------------------------------------------------===//
// Partitioned Copy Declarations
//===--------------------------------------------------------------------===//
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
	bool PreparedBatchesReady() const DUCKDB_REQUIRES(lock);

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

public:
	//! Partitioning-specific functions
	void InitializeFlush() DUCKDB_REQUIRES(lock);
	void FinalizeState(PartitionedCopyState &state, InterruptState &interrupt_state) DUCKDB_REQUIRES(state.lock);

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
	void RequestPartitionFileState(FileStateHandle &file_state, const vector<Value> &values,
	                               FileCreationReason reason = FileCreationReason::NORMAL)
	    DUCKDB_EXCLUDES(copy_gstate.lock);
	void FinalizeActiveWrites() DUCKDB_EXCLUDES(copy_gstate.lock);
	void FinalizeFileStates(vector<FileStateHandle> files_to_finalize) DUCKDB_EXCLUDES(copy_gstate.lock);

private:
	unique_ptr<const SortStrategy> ConstructSortStrategy() const;
	void CreateNextState();
	bool ShouldStopFlushing() const;
	bool RequiresSerializedPartitionWrites() const;
	void EnsureFreshPartitionFileForSortedRun(PartitionWriteInfo &write_info, const vector<Value> &values)
	    DUCKDB_EXCLUDES(copy_gstate.lock);
	void EnsureFreshPartitionFileForRotation(PartitionWriteInfo &write_info, const vector<Value> &values)
	    DUCKDB_EXCLUDES(copy_gstate.lock);
	//! Swaps write_info.file_state after temporarily dropping copy_gstate.lock to request the replacement file.
	//! Callers that can reach the swap path must serialize the full partition writer run for this write_info.
	void EnsureFreshPartitionFile(PartitionWriteInfo &write_info, const vector<Value> &values,
	                              FileCreationReason reason) DUCKDB_EXCLUDES(copy_gstate.lock);
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
	PartitionWriteManager partition_writes;

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

	//! Delayed below-threshold partitions
	DelayedPartitionBuffers delayed_partition_buffers;
};

//===--------------------------------------------------------------------===//
// Partition File Request Builder Declarations
//===--------------------------------------------------------------------===//
class PartitionFileRequestBuilder {
public:
	PartitionFileRequestBuilder(PartitionedCopy &partitioned_copy_p, FileStateHandle &file_state_p,
	                            const vector<Value> &values_p, FileCreationReason reason_p)
	    : partitioned_copy(partitioned_copy_p), file_state(file_state_p), values(values_p), reason(reason_p) {
	}

public:
	optional<PartitionFileRequest> Build();
	vector<FileStateHandle> TakeFilesToFinalize();

private:
	PartitionDirectory BuildDirectory(string path) const;

private:
	PartitionedCopy &partitioned_copy;
	FileStateHandle &file_state;
	const vector<Value> &values;
	FileCreationReason reason;
	PartitionFileStateReservation reservation;
};

//===--------------------------------------------------------------------===//
// Partitioned Copy Local State Declarations
//===--------------------------------------------------------------------===//
class PartitionedCopyLocalState : public LocalSinkState {
public:
	shared_ptr<PartitionedCopyState> current_state;
	unique_ptr<LocalSinkState> sort_strategy_local_state;
	idx_t append_count = 0;
};

//===--------------------------------------------------------------------===//
// Partitioned Copy Scoped Guard Declarations
//===--------------------------------------------------------------------===//
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

//===--------------------------------------------------------------------===//
// Implementations
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// Utility Helpers
//===--------------------------------------------------------------------===//
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

//===--------------------------------------------------------------------===//
// Copy File Lifecycle
//===--------------------------------------------------------------------===//
static void FinalizeLifecycleFileState(ClientContext &context, copy_to_finalize_t finalize, FunctionData &bind_data,
                                       unique_ptr<GlobalFileState> state) {
	if (!finalize) {
		throw InternalException("COPY file lifecycle finalize requires a finalize callback");
	}
	if (!state || !state->data) {
		throw InternalException("COPY file lifecycle finalize reached an empty file state");
	}
	finalize(context, bind_data, *state->data);
}
void CopyFileLifecycleExecutor::WaitForJob(CopyFileLifecycleJob &job, CopyFileLifecycleWaitMode mode) {
	while (!job.IsFinished()) {
		if (mode == CopyFileLifecycleWaitMode::INTERRUPTIBLE) {
			context.InterruptCheck();
		}
		WorkOnTaskOrYield();
	}
	job.Rethrow();
}

void CopyFileLifecycleExecutor::WaitAll(CopyFileLifecycleWaitMode mode) {
	if (mode == CopyFileLifecycleWaitMode::DRAIN) {
		executor.WorkOnTasks();
		ThrowError();
		return;
	}
	while (pending_tasks.load(std::memory_order_relaxed) > 0) {
		context.InterruptCheck();
		WorkOnTaskOrYield();
	}
	ThrowError();
}

void CopyFileLifecycleExecutor::FinishTask() {
	--pending_tasks;
}

void CopyFileLifecycleExecutor::PushError(const std::exception_ptr &error_p) {
	lock_guard<mutex> guard(error_lock);
	if (!error) {
		error = error_p;
	}
}

bool CopyFileLifecycleExecutor::WorkOnTask(bool throw_error) {
	shared_ptr<Task> task;
	if (!executor.GetTask(task)) {
		return false;
	}
	const auto result = task->Execute(TaskExecutionMode::PROCESS_ALL);
	D_ASSERT(result != TaskExecutionResult::TASK_BLOCKED);
	task.reset();
	if (throw_error) {
		ThrowError();
	}
	return true;
}

void CopyFileLifecycleExecutor::WorkOnTaskOrYield() {
	if (!WorkOnTask()) {
		TaskScheduler::YieldThread();
	}
}

void CopyFileLifecycleExecutor::WaitForTaskSlot(CopyFileLifecycleWaitMode mode) {
	while (pending_tasks >= max_pending_tasks) {
		if (mode == CopyFileLifecycleWaitMode::INTERRUPTIBLE) {
			context.InterruptCheck();
		}
		if (async_threads == 0 || mode == CopyFileLifecycleWaitMode::DRAIN) {
			WorkOnTaskOrYield();
		} else {
			TaskScheduler::YieldThread();
		}
	}
}

void CopyFileLifecycleExecutor::ThrowError() {
	lock_guard<mutex> guard(error_lock);
	if (error) {
		std::rethrow_exception(error);
	}
}

//===--------------------------------------------------------------------===//
// Copy File State Helpers
//===--------------------------------------------------------------------===//
void CopyDirectoryManager::EnsureDirectory(FileSystem &fs, const string &dir_path) {
	bool created_entry = false;
	{
		std::unique_lock<mutex> guard(lock);
		while (true) {
			auto entry = directories.find(dir_path);
			if (entry == directories.end()) {
				directories.emplace(dir_path, DirectoryEntry());
				created_entry = true;
				break;
			}

			if (entry->second.state == CopyDirectoryState::COMPLETE) {
				return;
			}
			if (entry->second.state == CopyDirectoryState::FAILED) {
				std::rethrow_exception(entry->second.error);
			}
			condition.wait(guard);
		}
	}

	std::exception_ptr error;
	try {
		if (!fs.DirectoryExists(dir_path)) {
			fs.CreateDirectory(dir_path);
		}
	} catch (...) {
		error = std::current_exception();
	}

	{
		lock_guard<mutex> guard(lock);
		auto entry = directories.find(dir_path);
		D_ASSERT(entry != directories.end());
		D_ASSERT(created_entry);
		entry->second.state = error ? CopyDirectoryState::FAILED : CopyDirectoryState::COMPLETE;
		entry->second.error = error;
	}
	condition.notify_all();

	if (error) {
		std::rethrow_exception(error);
	}
}

PendingFileState CopyOutputFileRegistry::ReserveFile(string output_path,
                                                     optional_ptr<const vector<Value>> partition_values) {
	PendingFileState result;
	result.output_path = std::move(output_path);

	if (op.return_type != CopyFunctionReturnType::CHANGED_ROWS) {
		result.written_file_info = AddFile(result.output_path);
	}

	if (result.written_file_info && !op.partition_columns.empty()) {
		D_ASSERT(partition_values);
		vector<Value> partition_keys;
		vector<Value> partition_values_as_varchar;
		for (idx_t i = 0; i < op.partition_columns.size(); i++) {
			const auto &partition_col_name = op.names[op.partition_columns[i]];
			const auto &partition_value = (*partition_values)[i];
			partition_keys.emplace_back(partition_col_name);
			partition_values_as_varchar.push_back(partition_value.DefaultCastAs(LogicalType::VARCHAR));
		}
		result.written_file_info->partition_keys =
		    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(partition_keys),
		               std::move(partition_values_as_varchar));
	}
	return result;
}

void CopyOutputFileRegistry::PublishCreatedPath(PendingFileState &pending_file_state, string output_path) {
	pending_file_state.output_path = std::move(output_path);
	if (pending_file_state.written_file_info) {
		pending_file_state.written_file_info->file_path = pending_file_state.output_path;
	}
	created_files.push_back(pending_file_state.output_path);
}

optional_ptr<CopyToFileInfo> CopyOutputFileRegistry::AddFile(const string &file_name) {
	auto file_info = make_uniq<CopyToFileInfo>(file_name);
	if (op.return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS) {
		file_info->file_stats = make_uniq<CopyFunctionFileStatistics>();
	}
	auto result = file_info.get();
	written_files.push_back(std::move(file_info));
	return result;
}

static bool PhysicalCopyRotateNow(const PhysicalCopyToFile &op, GlobalFileState &global_state)
    DUCKDB_REQUIRES(global_state.lock);

//===--------------------------------------------------------------------===//
// Partitioned Copy Scoped Guards
//===--------------------------------------------------------------------===//
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
// Partition Write Manager
//===--------------------------------------------------------------------===//
PartitionWriteLease::PartitionWriteLease(PartitionWriteManager &manager_p, PartitionWriteInfo &write_info_p)
    : manager(manager_p), write_info(write_info_p) {
}

PartitionWriteLease::PartitionWriteLease(PartitionWriteLease &&other) noexcept
    : manager(other.manager), write_info(other.write_info) {
	other.manager = nullptr;
	other.write_info = nullptr;
}

PartitionWriteLease &PartitionWriteLease::operator=(PartitionWriteLease &&other) noexcept {
	if (this != &other) {
		Reset();
		manager = other.manager;
		write_info = other.write_info;
		other.manager = nullptr;
		other.write_info = nullptr;
	}
	return *this;
}

PartitionWriteLease::~PartitionWriteLease() {
	Reset();
}

void PartitionWriteLease::Reset() {
	if (manager && write_info) {
		manager->Release(*write_info);
	}
	manager = nullptr;
	write_info = nullptr;
}

PartitionWriteLease PartitionWriteManager::Acquire(const vector<Value> &values) {
	PartitionWriteInfo *result;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto active_write_entry = active_writes.find(values);
		if (active_write_entry != active_writes.end()) {
			active_write_entry->second->active_writes++;
			result = active_write_entry->second.get();
		} else {
			auto info = make_uniq<PartitionWriteInfo>();
			result = info.get();
			info->active_writes = 1;
			active_writes.insert(make_pair(values, std::move(info)));
		}
	}
	return PartitionWriteLease(*this, *result);
}

PartitionWriteManager::ReservationLock PartitionWriteManager::LockForReservation() {
	return ReservationLock(lock);
}

PartitionFileStateReservation
PartitionWriteManager::ReserveFileState(ReservationLock &reservation_lock, const vector<Value> &values,
                                        FileCreationReason reason) DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
	D_ASSERT(reservation_lock.guard.owns_lock());
	PartitionFileStateReservation reservation;
	if (active_writes.size() >= Settings::Get<PartitionedWriteMaxOpenFilesSetting>(context)) {
		for (auto it = active_writes.begin(); it != active_writes.end(); ++it) {
			if (it->second->active_writes == 0) {
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

vector<FileStateHandle> PartitionWriteManager::TakeOpenFileStates() {
	vector<FileStateHandle> files_to_finalize;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		for (auto &entry : active_writes) {
			D_ASSERT(entry.second->active_writes == 0);
			files_to_finalize.push_back(std::move(entry.second->file_state));
		}
		active_writes.clear();
	}
	return files_to_finalize;
}

void PartitionWriteManager::Release(PartitionWriteInfo &write_info) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	D_ASSERT(write_info.active_writes > 0);
	write_info.active_writes--;
}

//===--------------------------------------------------------------------===//
// Partitioned Copy Hash Group
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
		if (prepared == count && PreparedBatchesReady()) {
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
	task.batch_idx = batch_state.NextCollectionIndex();
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
		auto action = batch_state.SelectPrepareTask(prepare_batch_idx);
		if (action.type == PartitionedCopyPrepareTaskActionType::SKIP_PARTITION) {
			++prepare_partition_idx;
			prepare_batch_idx = 0;
			continue;
		}
		if (action.type == PartitionedCopyPrepareTaskActionType::WAIT_FOR_WRITE_INFO) {
			return nullopt;
		}

		PartitionedCopyTask task;
		task.stage = PartitionedCopyStage::PREPARE;
		task.group_idx = group_idx;
		task.thread_idx = prepare_partition_idx;
		task.batch_idx = action.type == PartitionedCopyPrepareTaskActionType::ACQUIRE_WRITE_INFO
		                     ? DConstants::INVALID_INDEX
		                     : action.batch_idx;

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

bool PartitionedCopyHashGroup::PreparedBatchesReady() const {
	for (auto &batch_state_ptr : batch_states) {
		if (!batch_state_ptr) {
			return false;
		}
		auto &batch_state = *batch_state_ptr;
		if (!batch_state.ReadyForFlush()) {
			return false;
		}
	}
	return true;
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

	optional_ptr<PartitionWriteInfo> write_info;
	const auto flush_threshold = Settings::Get<PartitionedWriteFlushThresholdSetting>(partitioned_copy.context);
	const auto has_delayed_partition = partitioned_copy.HasDelayedPartition(values);
	PartitionedCopyBatchAction batch_action;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &batch_state = *batch_states[task.thread_idx];
		batch_action = batch_state.RegisterBatch(std::move(values), flush_threshold, has_delayed_partition);
	}

	const auto row_count = task.end_idx - task.begin_idx;
	if (batch_action.type == PartitionedCopyBatchActionType::STORE_COLLECTION) {
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			auto &current_batch_state = *batch_states[task.thread_idx];
			current_batch_state.StoreCollection(task.batch_idx, std::move(batch));
		}
		batched += row_count;
		return;
	}

	if (batch_action.type == PartitionedCopyBatchActionType::ACQUIRE_AND_PREPARE) {
		auto write_lease = partitioned_copy.partition_writes.Acquire(batch_action.values);
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			auto &current_batch_state = *batch_states[task.thread_idx];
			write_info = current_batch_state.CompleteWriteInfoAcquisition(std::move(write_lease));
		}
	} else {
		D_ASSERT(batch_action.type == PartitionedCopyBatchActionType::PREPARE_WITH_WRITE_INFO);
		write_info = batch_action.write_info;
	}
	D_ASSERT(write_info);
	auto prepared_batch = partitioned_copy.PreparePartitionBatch(
	    batch_action.values, *write_info, PartitionedCopyCollection(collection_schema, std::move(batch)));

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
		const auto has_delayed_partition = partitioned_copy.HasDelayedPartition(batch_state->Values());
		prepared += batch_state->FinalizeBatching(flush_threshold, has_delayed_partition);
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

	PartitionedCopyPrepareAction prepare_action;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &batch_state = *batch_states[task.thread_idx];
		prepare_action = batch_state.BeginPrepareTask(task.batch_idx);
	}

	if (prepare_action.type == PartitionedCopyPrepareActionType::ACQUIRE_WRITE_INFO) {
		auto write_lease = partitioned_copy.partition_writes.Acquire(prepare_action.values);
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &batch_state = *batch_states[task.thread_idx];
		batch_state.CompleteWriteInfoAcquisition(std::move(write_lease));
		return;
	}
	D_ASSERT(prepare_action.type == PartitionedCopyPrepareActionType::PREPARE_BATCH);
	D_ASSERT(prepare_action.data.collection);
	const auto row_count = prepare_action.data.Count();

	auto prepared_batch = partitioned_copy.PreparePartitionBatch(prepare_action.values, *prepare_action.write_info,
	                                                             std::move(prepare_action.data));

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

	PartitionedCopyFlushAction flush_action;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &batch_state = *batch_states[task.thread_idx];
		flush_action = batch_state.TakeFlushAction();
	}
	D_ASSERT(!flush_action.values.empty());

	if (flush_action.type == PartitionedCopyFlushActionType::DELAYED_COLLECTIONS) {
		const auto collection_schema = partitioned_copy.GetPartitionCollectionSchema();
		const auto &collection_types = partitioned_copy.GetPartitionCollectionTypes(collection_schema);
		auto collection = make_uniq<ColumnDataCollection>(partitioned_copy.context, collection_types);
		for (auto &batch : flush_action.collections) {
			D_ASSERT(batch.schema == collection_schema);
			if (batch.collection) {
				collection->Combine(*batch.collection);
			}
		}
		if (collection->Count() == 0) {
			return;
		}

		auto data = PartitionedCopyCollection(collection_schema, std::move(collection));
		auto ready_partition = partitioned_copy.BufferOrTakeReadyPartition(flush_action.values, std::move(data), false);
		if (ready_partition) {
			partitioned_copy.FlushPartitionCollection(execution_context, interrupt_state, std::move(*ready_partition));
		}
		return;
	}

	D_ASSERT(flush_action.type == PartitionedCopyFlushActionType::PREPARED_BATCHES);
	D_ASSERT(flush_action.write_lease);
	partitioned_copy.FlushPreparedPartitionRun(flush_action.values, *flush_action.write_lease,
	                                           std::move(flush_action.batches));
}

//===--------------------------------------------------------------------===//
// Partitioned Copy State
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

//===--------------------------------------------------------------------===//
// Partitioned Copy Lifecycle
//===--------------------------------------------------------------------===//
PartitionedCopy::PartitionedCopy(const PhysicalCopyToFile &op_p, ClientContext &context_p,
                                 CopyToFileGlobalState &copy_gstate_p)
    : op(op_p), context(context_p), copy_gstate(copy_gstate_p), partition_writes(op_p, context_p),
      sort_strategy(ConstructSortStrategy()), flushing(false), locals(0), combined(0), finalized(false) {
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
			partitioned_copy.copy_gstate.WaitForLifecycleTasks();
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
		copy_gstate.WaitForLifecycleTasks();
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
// Partition File Request Builder
//===--------------------------------------------------------------------===//
optional<PartitionFileRequest> PartitionFileRequestBuilder::Build() {
	auto reservation_lock = partitioned_copy.partition_writes.LockForReservation();
	annotated_lock_guard<annotated_mutex> global_guard(partitioned_copy.copy_gstate.lock);
	if (file_state) {
		return nullopt;
	}

	reservation = partitioned_copy.partition_writes.ReserveFileState(reservation_lock, values, reason);

	auto &op = partitioned_copy.op;
	auto &context = partitioned_copy.context;
	auto &fs = FileSystem::GetFileSystem(context);
	auto directory = BuildDirectory(op.GetTrimmedPath(context, op.file_path));
	auto full_path = op.filename_pattern.CreateFilename(fs, directory.path, op.file_extension, reservation.offset);
	auto pending_file_state_open =
	    partitioned_copy.copy_gstate.CreatePartitionFileStateOpenLocked(file_state, std::move(full_path), values);
	D_ASSERT(pending_file_state_open);

	PartitionFileOpenRequest open_request(std::move(pending_file_state_open), std::move(directory), reservation.offset);
	return PartitionFileRequest(std::move(open_request), std::move(reservation.files_to_finalize));
}

vector<FileStateHandle> PartitionFileRequestBuilder::TakeFilesToFinalize() {
	return std::move(reservation.files_to_finalize);
}

PartitionDirectory PartitionFileRequestBuilder::BuildDirectory(string path) const {
	auto &fs = FileSystem::GetFileSystem(partitioned_copy.context);
	PartitionDirectory result;
	result.path = std::move(path);
	if (partitioned_copy.op.hive_file_pattern) {
		for (idx_t i = 0; i < partitioned_copy.op.partition_columns.size(); i++) {
			const auto &partition_col_name = partitioned_copy.op.names[partitioned_copy.op.partition_columns[i]];
			const auto &partition_value = values[i];
			string p_dir;
			p_dir += HivePartitioning::Escape(partition_col_name.GetIdentifierName());
			p_dir += "=";
			if (partition_value.IsNull()) {
				p_dir += "__HIVE_DEFAULT_PARTITION__";
			} else {
				p_dir += HivePartitioning::Escape(partition_value.ToString());
			}
			result.path = fs.JoinPath(result.path, p_dir);
			result.directories.push_back(result.path);
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Partitioned Write Helpers
//===--------------------------------------------------------------------===//
unique_ptr<PartitionedCopyBatch> PartitionedCopy::PreparePartitionBatch(const vector<Value> &values,
                                                                        PartitionWriteInfo &write_info,
                                                                        PartitionedCopyCollection data) {
	const auto create_file_state_fun = [&](FileStateHandle &file_state) {
		RequestPartitionFileState(file_state, values);
	};
	create_file_state_fun(write_info.file_state);
	auto collection = PrepareCollectionForWrite(std::move(data));
	auto [batch_analyzer, prepared_batch] =
	    op.PrepareBatch(context, copy_gstate, write_info.file_state, create_file_state_fun, std::move(collection));
	return make_uniq<PartitionedCopyBatch>(batch_analyzer, std::move(prepared_batch));
}

void PartitionedCopy::FlushPreparedPartitionRun(const vector<Value> &values, PartitionWriteInfo &write_info,
                                                vector<unique_ptr<PartitionedCopyBatch>> batches) {
	WithSerializedPartitionWriteRun(write_info, [&]() {
		EnsureFreshPartitionFileForSortedRun(write_info, values);
		for (auto &batch : batches) {
			if (!batch) {
				throw InternalException("Partitioned COPY reached FLUSH with a missing prepared batch");
			}
			EnsureFreshPartitionFileForRotation(write_info, values);
			FlushPreparedPartitionBatch(values, write_info, std::move(batch));
		}
	});
}

void PartitionedCopy::FlushPreparedPartitionBatch(const vector<Value> &values, PartitionWriteInfo &write_info,
                                                  unique_ptr<PartitionedCopyBatch> batch) {
	D_ASSERT(batch);
	const auto create_file_state_fun = [&](FileStateHandle &file_state) {
		RequestPartitionFileState(file_state, values);
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

			auto write_lease = partition_writes.Acquire(flush.values);
			FlushDelayedPartitionRun(flush.values, *write_lease, *flush.data.collection);
		}

		auto next = delayed_guard.Complete();
		if (!next) {
			return;
		}
		flush = std::move(*next);
	}
}

void PartitionedCopy::RequestPartitionFileState(FileStateHandle &file_state, const vector<Value> &values,
                                                FileCreationReason reason) {
	PartitionFileRequestBuilder builder(*this, file_state, values, reason);
	optional<PartitionFileRequest> request;
	try {
		request = builder.Build();
	} catch (...) {
		auto error = std::current_exception();
		try {
			FinalizeFileStates(builder.TakeFilesToFinalize());
		} catch (...) {
		}
		std::rethrow_exception(error);
	}
	if (!request) {
		return;
	}

	auto open_job = request->OpenJob();
	try {
		FinalizeFileStates(std::move(request->files_to_finalize));
		copy_gstate.SchedulePartitionFileStateOpen(std::move(request->open_request));
	} catch (...) {
		if (open_job && !open_job->IsFinished()) {
			open_job->CompleteException(std::current_exception());
		}
		throw;
	}
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

	if (!write_info.file_state) {
		return;
	}
	auto &old_file_state_ref = copy_gstate.EnsureFileStateReady(
	    write_info.file_state, [&](FileStateHandle &file_state) { RequestPartitionFileState(file_state, values); });
	optional_ptr<GlobalFileState> old_file_state_ptr = old_file_state_ref;
	{
		annotated_lock_guard<annotated_mutex> global_guard(copy_gstate.lock);
		if (!write_info.file_state) {
			return;
		}
		D_ASSERT(RefersToSameObject(*old_file_state_ptr.get(), write_info.file_state.GetFileState()));
		annotated_lock_guard<annotated_mutex> file_guard(old_file_state_ptr->lock);
		if (reason == FileCreationReason::SORTED_RUN_BOUNDARY && old_file_state_ptr->num_batches == 0) {
			return;
		}
		if (reason == FileCreationReason::ROTATION && !PhysicalCopyRotateNow(op, *old_file_state_ptr)) {
			return;
		}
	}

	FileStateHandle new_file_state;
	RequestPartitionFileState(new_file_state, values, reason);

	FileStateHandle old_file_state;
	{
		annotated_lock_guard<annotated_mutex> global_guard(copy_gstate.lock);
		D_ASSERT(write_info.file_state);
		auto &current_file_state = write_info.file_state.GetFileState();
		D_ASSERT(RefersToSameObject(*old_file_state_ptr.get(), current_file_state));
		annotated_lock_guard<annotated_mutex> file_guard(current_file_state.lock);
		if (reason == FileCreationReason::SORTED_RUN_BOUNDARY) {
			D_ASSERT(current_file_state.num_batches > 0);
		} else {
			D_ASSERT(PhysicalCopyRotateNow(op, current_file_state));
		}

		old_file_state = std::move(write_info.file_state);
		write_info.file_state = std::move(new_file_state);
	}

	copy_gstate.FinalizeFileState(std::move(old_file_state));
}

void PartitionedCopy::FinalizeActiveWrites() {
	FinalizeFileStates(partition_writes.TakeOpenFileStates());
}

void PartitionedCopy::FinalizeFileStates(vector<FileStateHandle> files_to_finalize) {
	for (auto &file_state : files_to_finalize) {
		if (file_state) {
			copy_gstate.FinalizeFileState(std::move(file_state));
		}
	}
}

//===--------------------------------------------------------------------===//
// Copy Global State
//===--------------------------------------------------------------------===//
CopyToFileGlobalState::CopyToFileGlobalState(const PhysicalCopyToFile &op_p, ClientContext &context_p)
    : op(op_p), context(context_p), initialized(false), finalized(false), prepare_global_state(nullptr),
      create_file_state_fun([&](FileStateHandle &file_state) DUCKDB_EXCLUDES(lock) { RequestFileState(file_state); }),
      lifecycle_executor(context_p), output_files(op_p), rows_copied(0), last_file_offset(0) {
}

CopyToFileGlobalState::~CopyToFileGlobalState() {
	try {
		WaitForLifecycleTasks();
	} catch (...) {
	}
	if (!initialized || finalized || !output_files.HasCreatedFiles()) {
		return;
	}
	// If we reach here, the query failed before Finalize was called
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &file : output_files.GetCreatedFiles()) {
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
	RequestFileState(global_state, op.file_path);
	initialized = true;
}

void CopyToFileGlobalState::PrepareOutputDirectory() {
	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.IsRemoteFile(op.file_path)) {
		if (fs.FileExists(op.file_path)) {
			if (op.overwrite_mode == CopyOverwriteMode::COPY_OVERWRITE) {
				fs.RemoveFile(op.file_path);
			} else {
				throw IOException("Cannot write to \"%s\" - it exists and is a file, not a directory! Enable "
				                  "OVERWRITE option to overwrite the file",
				                  op.file_path);
			}
		}
	}

	if (!fs.DirectoryExists(op.file_path)) {
		fs.CreateDirectory(op.file_path);
	} else {
		CheckDirectory(fs, op.file_path, op.overwrite_mode);
	}
}

void CopyToFileGlobalState::ScheduleOutputDirectorySetup() {
	D_ASSERT(!output_directory_job);
	output_directory_job = make_shared_ptr<CopyFileLifecycleJob>();
	try {
		lifecycle_executor.Schedule(output_directory_job, CopyFileLifecycleWaitMode::INTERRUPTIBLE,
		                            [this]() { PrepareOutputDirectory(); });
	} catch (...) {
		if (!output_directory_job->IsFinished()) {
			output_directory_job->CompleteException(std::current_exception());
		}
		throw;
	}
}

void CopyToFileGlobalState::EnsureOutputDirectoryReady() {
	if (!output_directory_job) {
		return;
	}
	lifecycle_executor.WaitForJob(*output_directory_job, CopyFileLifecycleWaitMode::INTERRUPTIBLE);
}

void CopyToFileGlobalState::EnsureDirectory(const string &dir_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	directory_manager.EnsureDirectory(fs, dir_path);
}

PendingFileState CopyToFileGlobalState::PrepareFileStateLocked(string output_path,
                                                               optional_ptr<const vector<Value>> partition_values) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (output_path.empty()) {
		output_path = op.filename_pattern.CreateFilename(fs, op.file_path, op.file_extension, last_file_offset++);
	}
	auto result = output_files.ReserveFile(std::move(output_path), partition_values);
	output_files.PublishCreatedPath(result, result.output_path);
	return result;
}

void CopyToFileGlobalState::RegisterPendingFileStatePathLocked(PendingFileState &pending_file_state,
                                                               string output_path) {
	output_files.PublishCreatedPath(pending_file_state, std::move(output_path));
}

unique_ptr<GlobalFileState> CopyToFileGlobalState::InitializeFileState(PendingFileState pending_file_state) {
	auto data = op.function.copy_to_initialize_global(context, *op.bind_data, pending_file_state.output_path);
	if (pending_file_state.written_file_info && pending_file_state.written_file_info->file_stats) {
		op.function.copy_to_get_written_statistics(context, *op.bind_data, *data,
		                                           *pending_file_state.written_file_info->file_stats);
	}
	if (op.function.initialize_operator) {
		op.function.initialize_operator(*data, op);
	}

	return make_uniq<GlobalFileState>(std::move(data), pending_file_state.output_path);
}

void CopyToFileGlobalState::RegisterPrepareGlobalStateLocked(GlobalFileState &file_state) {
	if (!prepare_global_state.load(std::memory_order_acquire)) {
		prepare_global_state.store(file_state, std::memory_order_release);
	}
}

PendingFileStateOpen
CopyToFileGlobalState::CreateFileStateOpenLocked(FileStateHandle &file_state, string output_path,
                                                 optional_ptr<const vector<Value>> partition_values) {
	if (file_state.HasFileState()) {
		return PendingFileStateOpen();
	}
	PendingFileStateOpen result;
	result.pending_file_state = PrepareFileStateLocked(std::move(output_path), partition_values);
	result.open_job = make_shared_ptr<FileStateOpenJob>();
	file_state.open_job = result.open_job;
	return result;
}

PendingFileStateOpen
CopyToFileGlobalState::CreatePartitionFileStateOpenLocked(FileStateHandle &file_state, string output_path,
                                                          optional_ptr<const vector<Value>> partition_values) {
	if (file_state.HasFileState()) {
		return PendingFileStateOpen();
	}
	PendingFileStateOpen result;
	result.pending_file_state = output_files.ReserveFile(std::move(output_path), partition_values);
	result.open_job = make_shared_ptr<FileStateOpenJob>();
	file_state.open_job = result.open_job;
	return result;
}

void CopyToFileGlobalState::ScheduleFileStateOpen(PendingFileStateOpen pending_file_state_open) {
	D_ASSERT(pending_file_state_open);
	auto open_job = pending_file_state_open.open_job;
	try {
		lifecycle_executor.Schedule(
		    open_job, CopyFileLifecycleWaitMode::INTERRUPTIBLE,
		    [this, open_job, pending_file_state = std::move(pending_file_state_open.pending_file_state)]() mutable {
			    EnsureOutputDirectoryReady();
			    open_job->Complete(InitializeFileState(std::move(pending_file_state)));
		    });
	} catch (...) {
		if (!open_job->IsFinished()) {
			open_job->CompleteException(std::current_exception());
		}
		throw;
	}
}

void CopyToFileGlobalState::SchedulePartitionFileStateOpen(PartitionFileOpenRequest request) {
	auto open_job = request.open_job;
	D_ASSERT(open_job);
	try {
		lifecycle_executor.Schedule(open_job, CopyFileLifecycleWaitMode::INTERRUPTIBLE,
		                            [this, request = std::move(request)]() mutable { request.Run(*this); });
	} catch (...) {
		if (!open_job->IsFinished()) {
			open_job->CompleteException(std::current_exception());
		}
		throw;
	}
}

void PartitionFileOpenRequest::Run(CopyToFileGlobalState &copy_gstate) {
	copy_gstate.EnsureOutputDirectoryReady();
	auto &fs = FileSystem::GetFileSystem(copy_gstate.context);
	for (auto &dir : directory.directories) {
		copy_gstate.EnsureDirectory(dir);
	}

	auto output_path = std::move(pending_file_state.output_path);
	if (copy_gstate.op.overwrite_mode == CopyOverwriteMode::COPY_APPEND) {
		while (fs.FileExists(output_path)) {
			if (!copy_gstate.op.filename_pattern.HasUUID()) {
				throw InternalException("CopyOverwriteMode::COPY_APPEND without {uuid} - and file exists");
			}
			output_path = copy_gstate.op.filename_pattern.CreateFilename(fs, directory.path,
			                                                             copy_gstate.op.file_extension, offset);
		}
	}

	{
		annotated_lock_guard<annotated_mutex> guard(copy_gstate.lock);
		copy_gstate.RegisterPendingFileStatePathLocked(pending_file_state, std::move(output_path));
	}
	open_job->Complete(copy_gstate.InitializeFileState(std::move(pending_file_state)));
}

void CopyToFileGlobalState::RequestFileState(FileStateHandle &file_state, string output_path,
                                             optional_ptr<const vector<Value>> partition_values) {
	PendingFileStateOpen pending_file_state_open;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		pending_file_state_open = CreateFileStateOpenLocked(file_state, std::move(output_path), partition_values);
	}
	if (pending_file_state_open) {
		ScheduleFileStateOpen(std::move(pending_file_state_open));
	}
}

GlobalFileState &
CopyToFileGlobalState::EnsureFileStateReady(FileStateHandle &file_state,
                                            const std::function<void(FileStateHandle &)> &create_file_state_fun) {
	while (true) {
		shared_ptr<FileStateOpenJob> open_job;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			if (file_state.HasFileState()) {
				open_job = file_state.open_job;
			}
		}
		if (!open_job) {
			create_file_state_fun(file_state);
			continue;
		}
		lifecycle_executor.WaitForJob(*open_job, CopyFileLifecycleWaitMode::INTERRUPTIBLE);
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			if (file_state.open_job == open_job) {
				auto &result = open_job->GetFileState();
				RegisterPrepareGlobalStateLocked(result);
				return result;
			}
		}
	}
}

FileStateHandle CopyToFileGlobalState::FinalizeFileStateLocked(FileStateHandle file_state) {
	auto prepare_state = prepare_global_state.load(std::memory_order_acquire);
	if (prepare_state && RefersToSameObject(*prepare_state.get(), file_state.GetFileState())) {
		prepare_global_state_owned = std::move(file_state);
		return FileStateHandle();
	}
	return file_state;
}

void CopyToFileGlobalState::FinalizeFileState(FileStateHandle file_state) {
	if (!file_state) {
		return;
	}
	lifecycle_executor.WaitForJob(*file_state.open_job, CopyFileLifecycleWaitMode::DRAIN);
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		file_state = FinalizeFileStateLocked(std::move(file_state));
	}
	if (file_state) {
		auto finalize_job = make_shared_ptr<CopyFileLifecycleJob>();
		auto state = file_state.TakeFileState();
		auto state_holder = make_shared_ptr<unique_ptr<GlobalFileState>>(std::move(state));
		auto finalize = op.function.copy_to_finalize;
		auto &context_ref = context;
		auto &bind_data = *op.bind_data;
		try {
			lifecycle_executor.Schedule(finalize_job, CopyFileLifecycleWaitMode::DRAIN,
			                            [finalize, &context_ref, &bind_data, state_holder]() mutable {
				                            FinalizeLifecycleFileState(context_ref, finalize, bind_data,
				                                                       std::move(*state_holder));
			                            });
		} catch (...) {
			if (!finalize_job->IsFinished() && state_holder && *state_holder) {
				try {
					FinalizeLifecycleFileState(context_ref, finalize, bind_data, std::move(*state_holder));
				} catch (...) {
				}
			}
			throw;
		}
	}
}

FileStateHandle CopyToFileGlobalState::TryFinalizeOwnedFileStateLocked() {
	if (prepare_global_state_owned) {
		prepare_global_state.store(nullptr, std::memory_order_release);
		return std::move(prepare_global_state_owned);
	}
	return FileStateHandle();
}

void CopyToFileGlobalState::TryFinalizeOwnedFileState() {
	FileStateHandle file_state;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		file_state = TryFinalizeOwnedFileStateLocked();
	}
	if (file_state) {
		FinalizeFileState(std::move(file_state));
	}
}

void CopyToFileGlobalState::WaitForLifecycleTasks() {
	lifecycle_executor.WaitAll(CopyFileLifecycleWaitMode::DRAIN);
}

//===--------------------------------------------------------------------===//
// Copy Local State
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

PhysicalCopyToFile::~PhysicalCopyToFile() {
	sink_state.reset();
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

	// extra info map(varchar, variant)
	vector<Value> extra_keys;
	vector<Value> extra_values;
	for (auto &entry : file_stats.extra_info) {
		extra_keys.emplace_back(entry.first);
		extra_values.push_back(entry.second.DefaultCastAs(LogicalType::VARIANT()));
	}
	chunk.data[6].Append(
	    Value::MAP(LogicalType::VARCHAR, LogicalType::VARIANT(), std::move(extra_keys), std::move(extra_values)));
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
		auto state = make_uniq<CopyToFileGlobalState>(*this, context);
		state->ScheduleOutputDirectorySetup();
		if (!partition_output && !per_thread_output && Rotate() && write_empty_file) {
			state->RequestFileState(state->global_state);
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

	if (per_thread_output) {
		if (!lstate.global_file_state) {
			gstate.create_file_state_fun(lstate.global_file_state);
		}
	} else {
		gstate.create_file_state_fun(gstate.global_state);
	}
	auto &file_state = per_thread_output ? lstate.global_file_state : gstate.global_state;

	if (!lstate.batch) {
		lstate.batch = make_uniq<ColumnDataCollection>(context.client, expected_types);
		lstate.batch->InitializeAppend(lstate.batch_append_state);
	}
	lstate.batch->Append(lstate.batch_append_state, chunk);

	const CopyFunctionBatchAnalyzer batch_analyzer(*lstate.batch, batch_size, batch_size_bytes);
	if (batch_analyzer.MeetsFlushCriteria()) {
		lstate.batch_append_state.current_chunk_state.handles.clear();
		PrepareAndFlushBatch(context.client, gstate, file_state, gstate.create_file_state_fun, std::move(lstate.batch));
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
			gstate.RequestFileState(gstate.global_state);
		}
	}

	if (gstate.global_state) {
		gstate.FinalizeFileState(std::move(gstate.global_state));
	}

	gstate.TryFinalizeOwnedFileState();
	gstate.WaitForLifecycleTasks();

	if (use_tmp_file) {
		D_ASSERT(!per_thread_output);
		D_ASSERT(!partition_output);
		D_ASSERT(!file_size_bytes.IsValid());
		D_ASSERT(!Rotate());
		MoveTmpFile(context, file_path);
	}

	return SinkFinalizeType::READY;
}

void PhysicalCopyToFile::PrepareAndFlushBatch(ClientContext &context, GlobalSinkState &gstate_p,
                                              FileStateHandle &file_state,
                                              const std::function<void(FileStateHandle &)> &create_file_state_fun,
                                              unique_ptr<ColumnDataCollection> batch) const {
	auto [batch_analyzer, prepared_batch] =
	    PrepareBatch(context, gstate_p, file_state, create_file_state_fun, std::move(batch));
	FlushBatch(context, gstate_p, file_state, create_file_state_fun, batch_analyzer, std::move(prepared_batch));
}

//===--------------------------------------------------------------------===//
// Legacy Batch API
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
// Batch Interface
//===--------------------------------------------------------------------===//
pair<const CopyFunctionBatchAnalyzer, unique_ptr<PreparedBatchData>>
PhysicalCopyToFile::PrepareBatch(ClientContext &context, GlobalSinkState &gstate_p, FileStateHandle &file_state,
                                 const std::function<void(FileStateHandle &)> &create_file_state_fun,
                                 unique_ptr<ColumnDataCollection> batch) const {
	auto &gstate = gstate_p.Cast<CopyToFileGlobalState>();
	const CopyFunctionBatchAnalyzer batch_analyzer(*batch, batch_size, batch_size_bytes);

	if (UsesLegacyCopyBatchAPI(function)) {
		return {batch_analyzer, PrepareLegacyCopyBatch(std::move(batch))};
	}

	// Ensure we have a global state for prepares
	auto prepare_global_state = gstate.prepare_global_state.load(std::memory_order_acquire);
	if (!prepare_global_state) {
		gstate.EnsureFileStateReady(file_state, create_file_state_fun);
		prepare_global_state = gstate.prepare_global_state.load(std::memory_order_acquire);
		D_ASSERT(prepare_global_state);
	}

	// Prepare the batch
	return {batch_analyzer, function.prepare_batch(context, *bind_data, *prepare_global_state->data, std::move(batch))};
}

void PhysicalCopyToFile::FlushBatch(ClientContext &context, GlobalSinkState &gstate_p, FileStateHandle &file_state,
                                    const std::function<void(FileStateHandle &)> &create_file_state_fun,
                                    const CopyFunctionBatchAnalyzer &batch_analyzer,
                                    unique_ptr<PreparedBatchData> prepared_batch) const {
	auto &gstate = gstate_p.Cast<CopyToFileGlobalState>();

	while (true) {
		gstate.EnsureFileStateReady(file_state, create_file_state_fun);

		// Decide which file to flush to
		annotated_unique_lock<annotated_mutex> global_guard(gstate.lock);
		// Another thread may have rotated the file state since EnsureFileStateReady, so re-check readiness
		if (!file_state.IsReady()) {
			global_guard.unlock();
			gstate.lifecycle_executor.WorkOnTaskOrYield();
			continue;
		}

		auto ready_file_state = file_state.GetFileStatePtr();
		if (!ready_file_state) {
			global_guard.unlock();
			gstate.lifecycle_executor.WorkOnTaskOrYield();
			continue;
		}
		annotated_unique_lock<annotated_mutex> file_guard(ready_file_state->lock);
		if (PhysicalCopyRotateNow(*this, *ready_file_state)) {
			// Global state must be rotated. Move to local scope, create an new one, and immediately release global lock
			auto owned_file_state = std::move(file_state);
			file_guard.unlock();
			global_guard.unlock();

			gstate.EnsureFileStateReady(file_state, create_file_state_fun);

			// Finalize this file!
			gstate.FinalizeFileState(std::move(owned_file_state));
		} else {
			global_guard.unlock();
			ready_file_state->num_batches++;

			DUCKDB_LOG(context, PhysicalOperatorLogType, *this, "PhysicalCopyToFile", "FlushBatch",
			           {{"file", ready_file_state->path},
			            {"rows", to_string(batch_analyzer.current_batch_size)},
			            {"size", to_string(batch_analyzer.current_batch_size_bytes)},
			            {"reason", EnumUtil::ToString(batch_analyzer.ToReason())}});
			if (UsesLegacyCopyBatchAPI(function)) {
				FlushLegacyCopyBatch(context, function, *bind_data, *ready_file_state->data, *prepared_batch);
			} else {
				function.flush_batch(context, *bind_data, *ready_file_state->data, *prepared_batch);
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
	gstate.WaitForLifecycleTasks();
	annotated_lock_guard<annotated_mutex> global_guard(gstate.lock);
	if (return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS) {
		auto &source_state = input.global_state.Cast<CopyToFileGlobalSourceState>();
		idx_t next_end =
		    MinValue<idx_t>(source_state.offset + STANDARD_VECTOR_SIZE, gstate.output_files.WrittenFileCount());
		idx_t count = next_end - source_state.offset;
		for (idx_t i = 0; i < count; i++) {
			auto &file_entry = gstate.output_files.GetWrittenFile(source_state.offset + i);
			if (use_tmp_file) {
				file_entry.file_path = GetNonTmpFile(context.client, file_entry.file_path);
			}
			ReturnStatistics(chunk, file_entry);
		}
		source_state.offset += count;
		return source_state.offset < gstate.output_files.WrittenFileCount() ? SourceResultType::HAVE_MORE_OUTPUT
		                                                                    : SourceResultType::FINISHED;
	}

	switch (return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(gstate.rows_copied.load())));
		break;
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST: {
		chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(gstate.rows_copied.load())));
		vector<Value> file_name_list;
		for (auto &file_info : gstate.output_files.GetWrittenFiles()) {
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
