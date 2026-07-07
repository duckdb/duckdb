#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"

#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/serializer/async_memory_governor.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

// Async task that runs one scan job's I/O and releases the job's pending count when done.
class ReadAheadIOTask : public BaseExecutorTask {
public:
	ReadAheadIOTask(TaskExecutor &executor, unique_ptr<AsyncTask> task_p,
	                shared_ptr<ReadAheadJobCompletion> completion_p)
	    : BaseExecutorTask(executor), task(std::move(task_p)), completion(std::move(completion_p)) {
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

MultiFileReadAhead::MultiFileReadAhead(ClientContext &context, idx_t read_ahead_depth_p,
                                       unique_ptr<ManagedAsyncMemoryGovernor> memory_governor_p)
    : read_ahead_depth(read_ahead_depth_p), memory_governor(std::move(memory_governor_p)) {
	D_ASSERT(read_ahead_depth_p > 0);
	backlog_budget = memory_governor ? memory_governor->BackpressureBudget() : NumericLimits<idx_t>::Maximum();
	executor = make_uniq<TaskExecutor>(context, TaskSchedulerType::ASYNC);
}

unique_ptr<MultiFileReadAhead> MultiFileReadAhead::Create(ClientContext &context) {
	const auto configured_depth = Settings::Get<ReadAheadDepthSetting>(context);
	if (configured_depth == 0) {
		return nullptr;
	}
	if (configured_depth == -1) {
		// automatic mode, unlimited depth, the backlog is bounded by a temp-memory reservation instead
		return make_uniq<MultiFileReadAhead>(context, NumericLimits<idx_t>::Maximum(),
		                                     make_uniq<ManagedAsyncMemoryGovernor>(context));
	}
	return make_uniq<MultiFileReadAhead>(context, NumericCast<idx_t>(configured_depth), nullptr);
}

MultiFileReadAhead::~MultiFileReadAhead() {
	executor->CancelAndDrain();
}

void MultiFileReadAhead::SetDone() {
	done = true;
}

bool MultiFileReadAhead::IsDone() const {
	return done.load();
}

bool MultiFileReadAhead::TryReserveSlot() {
	const bool over_budget = pending_io_bytes.load() >= backlog_budget.load();
	const idx_t depth = over_budget ? 1 : read_ahead_depth;
	if (active_jobs.fetch_add(1) >= depth) {
		--active_jobs;
		return false;
	}
	++active_producers;
	return true;
}

bool MultiFileReadAhead::HasActiveProducers() const {
	return active_producers.load() > 0;
}

// Settles the reservation taken by TryReserveSlot
struct MultiFileReadAhead::ProducerReservation {
	explicit ProducerReservation(MultiFileReadAhead &read_ahead) : read_ahead(read_ahead) {
	}
	~ProducerReservation() {
		if (!committed) {
			read_ahead.ReleaseSlot();
		}
		--read_ahead.active_producers;
	}

	MultiFileReadAhead &read_ahead;
	bool committed = false;
};

bool MultiFileReadAhead::TryProduceJob(const ProduceJobCallback &claim_and_schedule) {
	ThrowIfError();
	if (IsDone() || !TryReserveSlot()) {
		return false;
	}
	ProducerReservation reservation(*this);
	try {
		auto job = make_uniq<MultiFileScanJob>();
		// prefer a finished job's scan state, so learned reader state carries over across jobs
		job->reader_scan_state = TryPopState();
		vector<unique_ptr<AsyncTask>> io_tasks;
		if (!claim_and_schedule(*job, io_tasks)) {
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

void MultiFileReadAhead::PushJob(unique_ptr<MultiFileScanJob> job, vector<unique_ptr<AsyncTask>> io_tasks) {
	// beyond its scheduled I/O a job carries scan-state overhead (row-group sized decode buffers)
	static constexpr idx_t MINIMUM_JOB_IO_CHARGE = 16ULL * 1024 * 1024;
	auto completion = make_shared_ptr<ReadAheadJobCompletion>(io_tasks.size());
	job->io_completion = completion;
	for (auto &task : io_tasks) {
		job->io_bytes += task->GetIOSize();
	}
	job->io_bytes = MaxValue<idx_t>(job->io_bytes, MINIMUM_JOB_IO_CHARGE);
	pending_io_bytes += job->io_bytes;
	// schedule the reads detached on the async pool right away
	for (auto &task : io_tasks) {
		executor->ScheduleTask(make_uniq<ReadAheadIOTask>(*executor, std::move(task), completion));
	}
	{
		lock_guard<mutex> guard(lock);
		if (memory_governor) {
			memory_governor->UpdateReservation(pending_io_bytes.load());
			backlog_budget = memory_governor->BackpressureBudget();
		}
		// producers push concurrently, so admit jobs to the queue in batch-index order
		pending_jobs.emplace(job->batch_index, std::move(job));
		while (!pending_jobs.empty() && pending_jobs.begin()->first == next_batch_index) {
			ready_queue.push_back(std::move(pending_jobs.begin()->second));
			pending_jobs.erase(pending_jobs.begin());
			next_batch_index++;
		}
	}
}

unique_ptr<MultiFileScanJob> MultiFileReadAhead::ClaimJob() {
	lock_guard<mutex> guard(lock);
	if (ready_queue.empty()) {
		return nullptr;
	}
	auto job = std::move(ready_queue.front());
	ready_queue.pop_front();
	ReleaseSlot();
	return job;
}

void MultiFileReadAhead::PushState(unique_ptr<LocalTableFunctionState> state) {
	D_ASSERT(state);
	lock_guard<mutex> guard(lock);
	state_pool.push_back(std::move(state));
}

unique_ptr<LocalTableFunctionState> MultiFileReadAhead::TryPopState() {
	lock_guard<mutex> guard(lock);
	if (state_pool.empty()) {
		return nullptr;
	}
	auto state = std::move(state_pool.back());
	state_pool.pop_back();
	return state;
}

void MultiFileReadAhead::WaitForJob(MultiFileScanJob &job) {
	if (job.io_completion) {
		while (job.io_completion->PendingIOTasks() > 0) {
			TaskScheduler::YieldThread();
		}
	}
	// the job's I/O has completed, release its budget charge
	pending_io_bytes -= job.io_bytes;
	job.io_bytes = 0;
	ThrowIfError();
}

void MultiFileReadAhead::PushError(ErrorData error) {
	executor->PushError(std::move(error));
}

void MultiFileReadAhead::ThrowIfError() {
	if (executor->HasError()) {
		executor->ThrowError();
	}
}

void MultiFileReadAhead::ReleaseSlot() {
	D_ASSERT(active_jobs.load() > 0);
	active_jobs--;
}

MultiFileGlobalState::MultiFileGlobalState(MultiFileList &file_list_p) : file_list(file_list_p) {
}

MultiFileGlobalState::MultiFileGlobalState(unique_ptr<MultiFileList> owned_file_list_p)
    : file_list(*owned_file_list_p), owned_file_list(std::move(owned_file_list_p)) {
}

MultiFileGlobalState::~MultiFileGlobalState() = default;

MultiFileLocalState::~MultiFileLocalState() {
	// job reads might still be going, wait for them before destroying ze job
	if (job_state == MultiFileJobState::WAIT_IO && job.io_completion) {
		while (job.io_completion->PendingIOTasks() > 0) {
			TaskScheduler::YieldThread();
		}
	}
}

} // namespace duckdb
