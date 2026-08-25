
#include "duckdb/parallel/scan_read_ahead.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

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

//! Throw when the query was interrupted, otherwise yield the thread
static void ScanReadAheadYield(ClientContext &context) {
	context.InterruptCheck();
	TaskScheduler::YieldThread();
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
		// If we are done we decrement the pending
		completion->FinishIOTask();
	}

	void ExecuteTask() override {
		// Does the actual IO
		task->Execute();
	}

private:
	unique_ptr<AsyncTask> task;
	shared_ptr<ReadAheadJobCompletion> completion;
};

ScanReadAheadJob::~ScanReadAheadJob() = default;

ScanReadAhead::ScanReadAhead(ClientContext &context, idx_t read_ahead_depth_p,
                             unique_ptr<ManagedAsyncMemoryGovernor> memory_governor_p)
    : read_ahead_depth(read_ahead_depth_p), memory_governor(std::move(memory_governor_p)) {
	D_ASSERT(read_ahead_depth_p > 0);
	backlog_budget = memory_governor ? memory_governor->BackpressureBudget() : NumericLimits<idx_t>::Maximum();
	executor = make_shared_ptr<TaskExecutor>(context, TaskSchedulerType::ASYNC);
}

ScanReadAhead::~ScanReadAhead() {
	executor->CancelAndDrain();
}

unique_ptr<ScanReadAhead> ScanReadAhead::Create(ClientContext &context) {
	if (TaskScheduler::GetScheduler(context).NumberOfAsyncThreads() == 0) {
		// read-ahead schedules its I/O on the async pool, without async threads there is nothing to gain
		return nullptr;
	}
	const auto configured_depth = Settings::Get<ReadAheadDepthSetting>(context);
	if (configured_depth == 0) {
		return nullptr;
	}
	if (configured_depth < 0) {
		// automatic mode, unlimited depth, the backlog is bounded by a temp-memory reservation instead
		return make_uniq<ScanReadAhead>(context, NumericLimits<idx_t>::Maximum(),
		                                make_uniq<ManagedAsyncMemoryGovernor>(context));
	}
	return make_uniq<ScanReadAhead>(context, NumericCast<idx_t>(configured_depth), nullptr);
}

bool ScanReadAhead::TryProduceJob(const ProduceJobCallback &claim_and_schedule) {
	ThrowIfError();
	if (IsDone() || !TryReserveSlot()) {
		return false;
	}
	ProducerReservation reservation(*this);
	try {
		vector<unique_ptr<AsyncTask>> io_tasks;
		auto job = claim_and_schedule(io_tasks);
		if (!job) {
			// there are no more jobs to produce, the scan is done
			SetDone();
			return false;
		}
		PushJob(std::move(job), std::move(io_tasks));
		reservation.committed = true;
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

unique_ptr<ScanReadAheadJob> ScanReadAhead::ClaimJob() {
	lock_guard<mutex> guard(lock);
	if (ready_queue.empty()) {
		return nullptr;
	}
	auto job = std::move(ready_queue.front());
	ready_queue.pop_front();
	ReleaseSlot();
	return job;
}

void ScanReadAhead::WaitForJob(ScanReadAheadJob &job) {
	job.SettleIO();
	// the job's I/O has completed, release its budget charge
	pending_io_bytes -= job.io_bytes;
	job.io_bytes = 0;
	ThrowIfError();
}

ScanReadAheadAcquire ScanReadAhead::AcquireJob(ClientContext &context, TableFunctionInput &data_p,
                                               const ProduceJobCallback &claim_and_schedule,
                                               unique_ptr<ScanReadAheadJob> &job) {
	D_ASSERT(!job);
	while (true) {
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
				return ScanReadAheadAcquire::EXHAUSTED;
			}
		}
		if (!job) {
			// another thread is between claiming an assignment and pushing its job, wait for it
			ScanReadAheadYield(context);
			continue;
		}
		// park until the job's scheduled I/O completes, the last I/O task to finish wakes the scan task
		if (job->io_completion->TryPark(data_p)) {
			return ScanReadAheadAcquire::PARKED;
		}
		// parking is not available to this caller or the I/O has already completed
		WaitForJob(*job);
		return ScanReadAheadAcquire::ACQUIRED;
	}
}

void ScanReadAhead::SetDone() {
	done = true;
}

bool ScanReadAhead::TryReserveSlot() {
	const bool over_budget = pending_io_bytes.load() >= backlog_budget.load();
	const idx_t depth = over_budget ? 1 : read_ahead_depth;
	if (active_jobs.fetch_add(1) >= depth) {
		--active_jobs;
		return false;
	}
	++active_producers;
	return true;
}

void ScanReadAhead::PushJob(unique_ptr<ScanReadAheadJob> job, vector<unique_ptr<AsyncTask>> io_tasks) {
	// beyond its scheduled I/O a job carries scan-state overhead (row-group sized decode buffers)
	static constexpr idx_t MINIMUM_JOB_IO_CHARGE = 16ULL * 1024 * 1024;
	auto completion = make_shared_ptr<ReadAheadJobCompletion>(executor);
	job->io_completion = completion;
	// wrap all reads before scheduling any, a wrapped task settles the completion even when scheduling throws
	vector<unique_ptr<Task>> read_tasks;
	read_tasks.reserve(io_tasks.size());
	for (auto &task : io_tasks) {
		job->io_bytes += task->GetIOSize();
		read_tasks.push_back(make_uniq<ReadAheadIOTask>(*executor, std::move(task), completion));
	}
	job->io_bytes = MaxValue<idx_t>(job->io_bytes, MINIMUM_JOB_IO_CHARGE);
	pending_io_bytes += job->io_bytes;
	// schedule the reads detached on the async pool right away
	for (auto &task : read_tasks) {
		executor->ScheduleTask(std::move(task));
	}
	lock_guard<mutex> guard(lock);
	if (memory_governor) {
		memory_governor->UpdateReservation(pending_io_bytes.load());
		backlog_budget = memory_governor->BackpressureBudget();
	}
	// producers push concurrently, so admit jobs to the queue in batch-index order
	if (job->batch_index < next_batch_index || !pending_jobs.emplace(job->batch_index, std::move(job)).second) {
		throw InternalException("Read-ahead jobs must have unique batch indexes, assigned densely from 0");
	}
	while (!pending_jobs.empty() && pending_jobs.begin()->first == next_batch_index) {
		ready_queue.push_back(std::move(pending_jobs.begin()->second));
		pending_jobs.erase(pending_jobs.begin());
		next_batch_index++;
	}
}

void ScanReadAhead::PushError(ErrorData error) {
	executor->PushError(std::move(error));
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

} // namespace duckdb
