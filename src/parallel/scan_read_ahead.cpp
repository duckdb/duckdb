
#include "duckdb/parallel/scan_read_ahead.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/scan_affinity_queue.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

ReadAheadJobCompletion::ReadAheadJobCompletion(shared_ptr<TaskExecutor> executor_p)
    : executor(std::move(executor_p)), pending_io_tasks(0) {
}

ReadAheadJobCompletion::~ReadAheadJobCompletion() = default;

void ReadAheadJobCompletion::OwnJob(ScanReadAhead &read_ahead_p, unique_ptr<ScanReadAheadJob> job) {
	D_ASSERT(!job->io_completion);
	read_ahead = read_ahead_p;
	ready_job = std::move(job);
}

void ReadAheadJobCompletion::AddIOTask() {
	++pending_io_tasks;
}

void ReadAheadJobCompletion::FinishIOTask() {
	const auto previous = pending_io_tasks.fetch_sub(1);
	D_ASSERT(previous > 0);
	if (previous > 1) {
		// I/O tasks still outstanding, nothing to wake yet
		return;
	}
	if (ready_job) {
		read_ahead->CompleteJob(std::move(ready_job));
		return;
	}
	// wake the parked scan task, if any
	const annotated_lock_guard<annotated_mutex> guard {parked_scan.lock};
	parked_scan.UnblockTasks();
}

bool ReadAheadJobCompletion::TryPark(const InterruptState &interrupt_state) {
	// checking the pending count under the same lock FinishIOTask takes before waking prevents lost wake-ups
	const annotated_lock_guard<annotated_mutex> guard {parked_scan.lock};
	if (pending_io_tasks.load() == 0) {
		return false;
	}
	return parked_scan.BlockTask(interrupt_state);
}

bool ReadAheadJobCompletion::TryPark(TableFunctionInput &data_p) {
	if (data_p.results_execution_mode != AsyncResultsExecutionMode::TASK_EXECUTOR || !data_p.interrupt_state) {
		return false;
	}
	if (!TryPark(*data_p.interrupt_state)) {
		return false;
	}
	data_p.async_result = AsyncResultType::BLOCKED;
	return true;
}

void ReadAheadJobCompletion::WaitForIO() {
	shared_ptr<Task> task;
	while (pending_io_tasks.load() > 0) {
		// run scheduled I/O inline, the tasks may belong to any job but always make progress
		if (executor->GetTask(task)) {
			task->Execute(TaskExecutionMode::PROCESS_ALL);
			task.reset();
			continue;
		}
		// the remaining I/O is running on other threads, wait for it to finish
		TaskScheduler::YieldThread();
	}
}

//! Async task that runs one scan job's I/O and releases the job's pending count when done.
class ReadAheadIOTask : public BaseExecutorTask {
public:
	ReadAheadIOTask(TaskExecutor &executor, unique_ptr<AsyncTask> task_p,
	                shared_ptr<ReadAheadJobCompletion> completion_p)
	    : BaseExecutorTask(executor), task(std::move(task_p)), completion(std::move(completion_p)) {
		completion->AddIOTask();
	}
	~ReadAheadIOTask() override {
		Finish();
	}

	void ExecuteTask() override {
		RunningRead running(completion->read_ahead);
		try {
			// Does the actual IO
			task->Execute();
		} catch (std::exception &ex) {
			executor.PushTaskError(ex);
		} catch (...) {
			executor.PushTaskError("Unknown exception during read-ahead I/O");
		}
		Finish();
	}

	void Cancel() override {
		Finish();
	}

private:
	//! Affinity waiters need a scan-progress wake-up before the executor counts this read as complete.
	class RunningRead {
	public:
		explicit RunningRead(optional_ptr<ScanReadAhead> read_ahead) : read_ahead(read_ahead) {
			if (read_ahead) {
				++read_ahead->running_reads;
			}
		}
		~RunningRead() {
			if (read_ahead && read_ahead->running_reads.fetch_sub(1) == 1) {
				// A parked consumer must retry even when the remaining reads have not started.
				read_ahead->SignalProgress(false);
			}
		}

	private:
		optional_ptr<ScanReadAhead> read_ahead;
	};

	void Finish() {
		if (!completion) {
			return;
		}
		// Publish before returning to executor completion accounting, including cancellation.
		task.reset();
		auto finished = std::move(completion);
		finished->FinishIOTask();
	}

	string TaskType() const override {
		return "ReadAheadIOTask";
	}

private:
	unique_ptr<AsyncTask> task;
	shared_ptr<ReadAheadJobCompletion> completion;
};

//! Async task that opens one file ahead of decoding
class FileOpenTask : public BaseExecutorTask {
public:
	FileOpenTask(TaskExecutor &executor, std::function<void()> open_fn_p, std::function<void()> cancel_fn_p)
	    : BaseExecutorTask(executor), open_fn(std::move(open_fn_p)), cancel_fn(std::move(cancel_fn_p)) {
	}

	void ExecuteTask() override {
		open_fn();
	}

	//! The open never runs, so settle the state the scan waits on - it only ever leaves OPENING in one of these two
	void Cancel() override {
		cancel_fn();
	}

	string TaskType() const override {
		return "FileOpenTask";
	}

private:
	std::function<void()> open_fn;
	std::function<void()> cancel_fn;
};

ScanReadAheadJob::~ScanReadAheadJob() = default;

//! The file descriptor limit is per process, so all scans share one count
static atomic<idx_t> &OpenFileCount() {
	static atomic<idx_t> count {0};
	return count;
}

ReadAheadOpenFile::ReadAheadOpenFile() {
	++OpenFileCount();
}

ReadAheadOpenFile::~ReadAheadOpenFile() {
	--OpenFileCount();
}

idx_t ReadAheadOpenFile::Count() {
	return OpenFileCount().load();
}

static idx_t GetOpenFileBudget(ClientContext &context) {
	idx_t file_descriptor_limit = Settings::Get<DebugFileDescriptorLimitSetting>(context);
	if (file_descriptor_limit == 0) {
		auto system_limit = FileSystem::GetFileDescriptorLimit();
		if (!system_limit.IsValid()) {
			return NumericLimits<idx_t>::Maximum();
		}
		file_descriptor_limit = system_limit.GetIndex();
	}
	// files hold their descriptors until their readers close, so only use a share of the limit
	return FileSystem::GetFileDescriptorShare(file_descriptor_limit);
}

idx_t ScanReadAhead::JobWindow(ScanReadAheadPolicy policy, idx_t read_ahead_depth, idx_t scan_concurrency) {
	if (policy == ScanReadAheadPolicy::ORDERED) {
		return read_ahead_depth;
	}
	// Pending reads also occupy slots for work that would otherwise be held by scan workers.
	const auto depth = MinValue<idx_t>(read_ahead_depth, AFFINITY_WINDOW);
	return depth + MinValue<idx_t>(scan_concurrency, AFFINITY_WINDOW - depth);
}

ScanReadAhead::ScanReadAhead(ClientContext &context, ScanReadAheadPolicy policy, idx_t read_ahead_depth,
                             unique_ptr<ManagedAsyncMemoryGovernor> memory_governor_p, bool prefetch_enabled_p,
                             idx_t shard_count, idx_t scan_concurrency)
    : context(context), prefetch_enabled(prefetch_enabled_p),
      job_window(JobWindow(policy, read_ahead_depth, scan_concurrency)), memory_governor(std::move(memory_governor_p)),
      open_window(MaxValue<idx_t>(TaskScheduler::GetScheduler(context).NumberOfAsyncThreads() * 2,
                                  memory_governor ? 0 : read_ahead_depth)),
      open_file_budget(GetOpenFileBudget(context)),
      affinity(policy == ScanReadAheadPolicy::AFFINITY ? make_uniq<ScanAffinityQueue>(shard_count) : nullptr) {
	D_ASSERT(read_ahead_depth > 0);
	if (affinity && memory_governor) {
		memory_governor->UpdateReservation(MINIMUM_AFFINITY_JOB_CHARGE * AFFINITY_WINDOW);
	}
	backlog_budget = memory_governor ? memory_governor->BackpressureBudget() : NumericLimits<idx_t>::Maximum();
	executor = make_shared_ptr<TaskExecutor>(context, TaskSchedulerType::ASYNC);
}

ScanReadAhead::~ScanReadAhead() {
	CancelAndDrain();
}

void ScanReadAhead::CancelAndDrain() {
	done = true;
	executor->CancelAndDrain();
}

unique_ptr<ScanReadAhead> ScanReadAhead::Create(ClientContext &context, ScanReadAheadPolicy policy, bool prefetch,
                                                idx_t shard_count, idx_t scan_concurrency) {
	const auto configured_depth = Settings::Get<ReadAheadDepthSetting>(context);
	prefetch = prefetch && TaskScheduler::GetScheduler(context).NumberOfAsyncThreads() > 0 && configured_depth != 0;
	if (!prefetch) {
		if (policy == ScanReadAheadPolicy::AFFINITY) {
			return make_uniq<ScanReadAhead>(context, policy, AFFINITY_WINDOW, nullptr, false, shard_count,
			                                scan_concurrency);
		}
		return nullptr;
	}
	if (configured_depth < 0) {
		// automatic mode, unlimited depth, the backlog is bounded by a temp-memory reservation instead
		return make_uniq<ScanReadAhead>(context, policy, NumericLimits<idx_t>::Maximum(),
		                                make_uniq<ManagedAsyncMemoryGovernor>(context.GetContext()), true, shard_count,
		                                scan_concurrency);
	}
	return make_uniq<ScanReadAhead>(context, policy, NumericCast<idx_t>(configured_depth), nullptr, true, shard_count,
	                                scan_concurrency);
}

idx_t ScanReadAhead::MinimumJobCharge() const {
	if (!affinity) {
		return MINIMUM_ORDERED_JOB_CHARGE;
	}
	// A fixed assignment window bounds metadata when no memory governor is in use.
	return memory_governor ? MINIMUM_AFFINITY_JOB_CHARGE : 0;
}

idx_t ScanReadAhead::ReserveBatch() {
	const auto minimum = MinimumJobCharge();
	auto reserved = PRODUCE_BATCH;
	const auto previous_bytes = minimum ? pending_budget_charge.fetch_add(reserved * minimum) : 0;
	const auto depth = ReservationDepth(previous_bytes);
	auto count = active_jobs.load(std::memory_order_relaxed);
	while (count < depth) {
		const auto slots = MinValue<idx_t>(reserved, depth - count);
		if (active_jobs.compare_exchange_weak(count, count + slots, std::memory_order_relaxed)) {
			if (minimum) {
				pending_budget_charge -= (reserved - slots) * minimum;
			}
			++active_producers;
			return slots;
		}
	}
	if (minimum) {
		pending_budget_charge -= reserved * minimum;
	}
	return 0;
}

bool ScanReadAhead::TryProduceBatch(const ProduceJobCallback &produce, idx_t local_shard) {
	ThrowIfError();
	if (IsDone()) {
		return false;
	}
	const auto count = ReserveBatch();
	if (!count) {
		return false;
	}
	ProducerReservation reservation(*this, count);
	try {
		for (idx_t i = 0; i < count; i++) {
			ThrowIfError();
			vector<unique_ptr<AsyncTask>> io_tasks;
			auto job = produce(io_tasks);
			if (!job) {
				ThrowIfError();
				SetDone();
				break;
			}
			job->producer_shard = local_shard;
			ScheduleReadyJob(std::move(job), std::move(io_tasks), reservation);
			if (pending_budget_charge.load() >= backlog_budget.load()) {
				break;
			}
		}
	} catch (std::exception &ex) {
		executor->PushTaskError(ex);
		SignalProgress(true);
		throw;
	} catch (...) {
		executor->PushTaskError("Unknown exception while producing read-ahead work");
		SignalProgress(true);
		throw;
	}
	return true;
}

void ScanReadAhead::ScheduleReadyJob(unique_ptr<ScanReadAheadJob> job, vector<unique_ptr<AsyncTask>> io_tasks,
                                     ProducerReservation &reservation) {
	const auto minimum = MinimumJobCharge();
	job->budget_charge = memory_governor ? MaxValue<idx_t>(minimum, job->GetMemoryUsage()) : 0;
	for (auto &task : io_tasks) {
		if (memory_governor) {
			job->budget_charge += task->GetIOSize();
		}
	}
	if (io_tasks.empty()) {
		if (job->budget_charge > minimum) {
			pending_budget_charge += job->budget_charge - minimum;
		}
		reservation.Commit();
		try {
			UpdateBudget();
		} catch (...) {
			RetireJob(job->budget_charge);
			throw;
		}
		CompleteJob(std::move(job));
		return;
	}
	auto completion = make_shared_ptr<ReadAheadJobCompletion>(executor);
	// Hold publication until all wrappers, submission and producer bookkeeping are finished.
	completion->AddIOTask();
	struct PublishGuard {
		explicit PublishGuard(ReadAheadJobCompletion &completion) : completion(completion) {
		}
		~PublishGuard() {
			completion.FinishIOTask();
		}
		ReadAheadJobCompletion &completion;
	} publish(*completion);
	vector<unique_ptr<BaseExecutorTask>> read_tasks;
	read_tasks.reserve(io_tasks.size());
	for (auto &task : io_tasks) {
		read_tasks.push_back(make_uniq<ReadAheadIOTask>(*executor, std::move(task), completion));
	}
	if (job->budget_charge > minimum) {
		pending_budget_charge += job->budget_charge - minimum;
	}
	reservation.Commit();
	completion->OwnJob(*this, std::move(job));
	try {
		UpdateBudget();
		DUCKDB_LOG(context, AsyncTaskScheduleLogType, "READ_AHEAD", read_tasks.size());
		executor->ScheduleTasks(std::move(read_tasks));
	} catch (std::exception &ex) {
		executor->PushTaskError(ex);
		throw;
	} catch (...) {
		executor->PushTaskError("Unknown exception while scheduling read-ahead I/O");
		throw;
	}
}

void ScanReadAhead::RetireJob(idx_t budget_charge) {
	if (budget_charge) {
		pending_budget_charge -= budget_charge;
	}
	ReleaseSlot();
	SignalProgress(true);
}

void ScanReadAhead::CompleteJob(unique_ptr<ScanReadAheadJob> job) {
	const auto budget_charge = job->budget_charge;
	if (executor->IsCancelled() || executor->HasError()) {
		RetireJob(budget_charge);
		return;
	}
	try {
		const auto hint = job->ResolveAffinity();
		if (executor->IsCancelled() || executor->HasError()) {
			RetireJob(budget_charge);
			return;
		}
		const auto producer_shard = job->producer_shard;
		affinity->Push(producer_shard, hint, std::move(job));
	} catch (std::exception &ex) {
		executor->PushTaskError(ex);
		RetireJob(budget_charge);
		return;
	} catch (...) {
		executor->PushTaskError("Unknown exception while publishing read-ahead work");
		RetireJob(budget_charge);
		return;
	}
	SignalProgress(false);
}

bool ScanReadAhead::TryParkForProgress(TableFunctionInput &input, idx_t epoch) {
	lock_guard<mutex> guard(lock);
	parked_scans.push_back(*input.interrupt_state);
	++waiting_threads;
	// Register before rechecking: a publisher can skip the lock while there are no waiters.
	if (ProgressEpoch() != epoch || running_reads.load() == 0) {
		parked_scans.pop_back();
		--waiting_threads;
		return false;
	}
	input.async_result = AsyncResultType::BLOCKED;
	return true;
}

ScanReadAheadAcquire ScanReadAhead::AcquireReadyJob(ClientContext &context, TableFunctionInput &input,
                                                    const ProduceJobCallback &produce,
                                                    unique_ptr<ScanReadAheadJob> &job, idx_t local_shard) {
	D_ASSERT(affinity && !job);
	while (true) {
		const auto epoch = ProgressEpoch();
		TryProduceBatch(produce, local_shard);
		ThrowIfError();
		if (affinity->TryPop(local_shard, job)) {
			ReleaseSlot();
			WaitForJob(*job);
			SignalProgress(false);
			return ScanReadAheadAcquire::ACQUIRED;
		}
		if (IsDone() && !HasActiveProducers() && active_jobs.load() == 0) {
			ReleaseMemoryReservation();
			return ScanReadAheadAcquire::EXHAUSTED;
		}
		context.InterruptCheck();
		if (!IsDone() && active_jobs.load() < ReservationDepth(pending_budget_charge.load())) {
			continue;
		}
		if (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR && input.interrupt_state &&
		    input.interrupt_state->CanCallback() && running_reads.load() > 0) {
			if (TryParkForProgress(input, epoch)) {
				return ScanReadAheadAcquire::PARKED;
			}
			continue;
		}
		// Without an executing read, keep progress independent of the worker pools.
		if (TryRunPendingTask()) {
			continue;
		}
		WaitForProgress(context, epoch);
	}
}

bool ScanReadAhead::TryProduceJob(const ProduceJobCallback &claim_and_schedule) {
	ThrowIfError();
	if (IsDone() || !TryReserveSlot()) {
		return false;
	}
	ProducerReservation reservation(*this, 1);
	try {
		vector<unique_ptr<AsyncTask>> io_tasks;
		auto job = claim_and_schedule(io_tasks);
		if (!job) {
			// an async open that failed while we were claiming must fail the scan, not quiet-finish it
			ThrowIfError();
			SetDone();
			return false;
		}
		PushJob(std::move(job), std::move(io_tasks));
		reservation.Commit();
	} catch (std::exception &ex) {
		PushError(ErrorData(ex));
		throw;
	} catch (...) { // LCOV_EXCL_START
		PushError(ErrorData("Unknown exception while producing a read-ahead job"));
		throw;
	} // LCOV_EXCL_STOP
	return true;
}

bool ScanReadAhead::IsDone() const {
	return done.load();
}

bool ScanReadAhead::HasActiveProducers() const {
	return active_producers.load() > 0;
}

void ScanReadAhead::ReleaseMemoryReservation() {
	// a finished scan returns its reservation now, not at query end, so later joins and aggregates can use it
	if (!memory_governor) {
		return;
	}
	memory_governor->Release();
	backlog_budget = 0;
}

unique_ptr<ScanReadAheadJob> ScanReadAhead::ClaimJob() {
	lock_guard<mutex> guard(lock);
	if (ready_queue.empty()) {
		return nullptr;
	}
	auto job = std::move(ready_queue.front());
	ready_queue.pop_front();
	ReleaseSlot();
	NotifyProgress(false);
	return job;
}

void ScanReadAhead::WaitForJob(ScanReadAheadJob &job) {
	job.SettleIO();
	if (!job.budget_charge) {
		ThrowIfError();
		return;
	}
	// the job's I/O has completed, release its budget charge
	const auto budget = backlog_budget.load();
	const auto previous = pending_budget_charge.fetch_sub(job.budget_charge);
	if (previous >= budget && previous - job.budget_charge < budget) {
		// the scan is no longer over budget, a waiting thread can produce
		SignalProgress(false);
	}
	job.budget_charge = 0;
	ThrowIfError();
}

ScanReadAheadAcquire ScanReadAhead::AcquireJob(ClientContext &context, TableFunctionInput &data_p,
                                               const ProduceJobCallback &claim_and_schedule,
                                               unique_ptr<ScanReadAheadJob> &job) {
	D_ASSERT(!affinity && !job);
	optional<std::chrono::steady_clock::time_point> yield_deadline;
	while (true) {
		const auto epoch = ProgressEpoch();
		// keep the queue full before claiming
		while (TryProduceJob(claim_and_schedule)) {
		}
		job = ClaimJob();
		if (!job && IsDone() && !HasActiveProducers()) {
			// re-check the queue, a job may have been pushed between the claim and the done check
			job = ClaimJob();
			if (!job) {
				if (HasPendingJobs()) {
					// with no producers left the held-back jobs can never be admitted, their rows would be lost
					throw InternalException("Read-ahead exhausted with jobs still held back, "
					                        "batch indexes must be gapless from 0");
				}
				ReleaseMemoryReservation();
				return ScanReadAheadAcquire::EXHAUSTED;
			}
		}
		if (!job) {
			// another thread is between claiming an assignment and pushing its job, a short claim is caught by
			// yielding and a long one is waited for
			const auto now = std::chrono::steady_clock::now();
			if (!yield_deadline) {
				yield_deadline = now + std::chrono::microseconds(PRODUCER_YIELD_MICROS);
			}
			if (now < *yield_deadline) {
				context.FastInterruptCheck();
				TaskScheduler::YieldThread();
			} else {
				WaitForProgress(context, epoch);
			}
			continue;
		}
		// park until the job's scheduled I/O completes, the last I/O task to finish wakes the scan task
		// a parked task frees its thread for one that opens another file, so not while the open file budget is full
		if (CanScheduleOpen(0) && job->io_completion->TryPark(data_p)) {
			return ScanReadAheadAcquire::PARKED;
		}
		// parking is not available to this caller, the open file budget is full or the I/O has already completed
		WaitForJob(*job);
		return ScanReadAheadAcquire::ACQUIRED;
	}
}

void ScanReadAhead::SetDone() {
	done = true;
}

idx_t ScanReadAhead::ReservationDepth(idx_t pending_bytes) const {
	// Keep one job in flight while over budget so a consumer can still make progress.
	const bool over_budget = pending_bytes >= backlog_budget.load() || ReadAheadOpenFile::Count() >= open_file_budget;
	return over_budget ? 1 : job_window;
}

void ScanReadAhead::UpdateBudget() {
	const auto minimum = MinimumJobCharge();
	if (memory_governor && pending_budget_charge.load() + minimum > backlog_budget.load()) {
		lock_guard<mutex> guard(lock);
		memory_governor->UpdateReservation(pending_budget_charge.load() + minimum);
		backlog_budget = memory_governor->BackpressureBudget();
	}
	DUCKDB_LOG(context, ReadAheadBudgetLogType, pending_budget_charge.load(), backlog_budget.load(),
	           active_jobs.load());
}

bool ScanReadAhead::TryReserveSlot() {
	// take the minimum job charge up front so concurrent producers cannot all pass the budget check together
	const auto previous_bytes = pending_budget_charge.fetch_add(MINIMUM_ORDERED_JOB_CHARGE);
	const auto depth = ReservationDepth(previous_bytes);
	if (active_jobs.fetch_add(1) >= depth) {
		--active_jobs;
		pending_budget_charge -= MINIMUM_ORDERED_JOB_CHARGE;
		if (active_jobs.load() < ReservationDepth(pending_budget_charge.load())) {
			// a concurrent attempt failed on this attempt's charge or slot, let it retry
			SignalProgress(false);
		}
		return false;
	}
	++active_producers;
	return true;
}

void ScanReadAhead::PushJob(unique_ptr<ScanReadAheadJob> job, vector<unique_ptr<AsyncTask>> io_tasks) {
	auto completion = make_shared_ptr<ReadAheadJobCompletion>(executor);
	job->io_completion = completion;
	// wrap all reads before scheduling any, a wrapped task settles the completion even when scheduling throws
	vector<unique_ptr<BaseExecutorTask>> read_tasks;
	read_tasks.reserve(io_tasks.size());
	for (auto &task : io_tasks) {
		job->budget_charge += task->GetIOSize();
		read_tasks.push_back(make_uniq<ReadAheadIOTask>(*executor, std::move(task), completion));
	}
	job->budget_charge = MaxValue<idx_t>(job->budget_charge, MINIMUM_ORDERED_JOB_CHARGE);
	// the minimum charge was taken by TryReserveSlot, charge the remainder
	pending_budget_charge += job->budget_charge - MINIMUM_ORDERED_JOB_CHARGE;
	// schedule the reads detached on the async pool right away, logged under their own label
	if (!read_tasks.empty()) {
		DUCKDB_LOG(context, AsyncTaskScheduleLogType, "READ_AHEAD", read_tasks.size());
	}
	executor->ScheduleTasks(std::move(read_tasks));
	UpdateBudget();
	lock_guard<mutex> guard(lock);
	// producers push concurrently, so admit jobs to the queue in batch-index order
	if (job->batch_index < next_batch_index || !pending_jobs.emplace(job->batch_index, std::move(job)).second) {
		throw InternalException("Read-ahead jobs must have unique batch indexes, assigned densely from 0");
	}
	idx_t admitted = 0;
	while (!pending_jobs.empty() && pending_jobs.begin()->first == next_batch_index) {
		ready_queue.push_back(std::move(pending_jobs.begin()->second));
		pending_jobs.erase(pending_jobs.begin());
		next_batch_index++;
		admitted++;
	}
	NotifyProgress(admitted > 1);
}

void ScanReadAhead::PushError(ErrorData error) {
	executor->PushError(std::move(error));
	SignalProgress(true);
}

void ScanReadAhead::ScheduleFileOpen(std::function<void()> open_fn, std::function<void()> cancel_fn) {
	D_ASSERT(prefetch_enabled);
	executor->ScheduleTask(make_uniq<FileOpenTask>(*executor, std::move(open_fn), std::move(cancel_fn)));
}

bool ScanReadAhead::CanScheduleOpen(idx_t files_ahead) const {
	return prefetch_enabled && files_ahead < open_window && ReadAheadOpenFile::Count() < open_file_budget;
}

bool ScanReadAhead::TryRunPendingTask() {
	shared_ptr<Task> task;
	if (!executor->GetTask(task)) {
		return false;
	}
	task->Execute(TaskExecutionMode::PROCESS_ALL);
	return true;
}

void ScanReadAhead::ThrowIfError() {
	if (executor->HasError()) {
		executor->ThrowError();
	}
}

void ScanReadAhead::ReleaseSlot() {
	D_ASSERT(active_jobs.load() > 0);
	active_jobs--;
}

void ScanReadAhead::WakeWaiters(bool wake_all) {
	if (wake_all) {
		progress.notify_all();
	} else {
		progress.notify_one();
	}
}

void ScanReadAhead::NotifyProgress(bool wake_all) {
	++progress_epoch;
	if (waiting_threads.load() > 0) {
		WakeWaiters(wake_all);
	}
}

void ScanReadAhead::SignalProgress(bool wake_all) {
	++progress_epoch;
	if (waiting_threads.load() == 0) {
		return;
	}
	optional<InterruptState> wake_one;
	vector<InterruptState> wake_many;
	{
		lock_guard<mutex> guard(lock);
		wake_all |= IsDone() && !HasActiveProducers() && active_jobs.load() == 0;
		if (wake_all) {
			wake_many.swap(parked_scans);
			waiting_threads -= wake_many.size();
		} else if (!parked_scans.empty()) {
			wake_one = std::move(parked_scans.back());
			parked_scans.pop_back();
			--waiting_threads;
		}
	}
	WakeWaiters(wake_all);
	if (wake_one) {
		ResumeScan(*wake_one);
	}
	for (auto &interrupt_state : wake_many) {
		ResumeScan(interrupt_state);
	}
}

void ScanReadAhead::ResumeScan(const InterruptState &interrupt_state) {
	try {
		interrupt_state.Callback();
	} catch (std::exception &ex) {
		executor->PushTaskError(ex);
		Executor::Get(context).PushTaskError(ex);
	} catch (...) {
		const auto message = "Unknown exception while resuming a scan";
		executor->PushTaskError(message);
		Executor::Get(context).PushTaskError(message);
	}
}

void ScanReadAhead::WaitForProgress(ClientContext &context, idx_t epoch) {
	context.InterruptCheck();
	{
		unique_lock<mutex> guard(lock);
		++waiting_threads;
		// the timeout bounds the interrupt latency
		progress.wait_for(guard, std::chrono::milliseconds(10), [&]() { return progress_epoch.load() != epoch; });
		--waiting_threads;
	}
	context.InterruptCheck();
}

idx_t ScanReadAhead::ProgressEpoch() const {
	return progress_epoch.load();
}

} // namespace duckdb
