#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"

#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

// Async task that runs one scan job's I/O and releases the job's pending count when done.
class ReadAheadIOTask : public BaseExecutorTask {
public:
	ReadAheadIOTask(TaskExecutor &executor, unique_ptr<AsyncTask> task_p, shared_ptr<atomic<idx_t>> pending_p)
	    : BaseExecutorTask(executor), task(std::move(task_p)), pending(std::move(pending_p)) {
	}
	~ReadAheadIOTask() override {
		// If we are done we decrement the pending
		--(*pending);
	}

	void ExecuteTask() override {
		// Does the actual IO
		task->Execute();
	}

private:
	unique_ptr<AsyncTask> task;
	shared_ptr<atomic<idx_t>> pending;
};

MultiFileReadAhead::MultiFileReadAhead(ClientContext &context, idx_t read_ahead_depth_p)
    : read_ahead_depth(read_ahead_depth_p),
      io_byte_budget(Settings::Get<ReadAheadDepthSetting>(context) == -1
                         ? BufferManager::GetBufferManager(context).GetMaxMemory() / 4
                         : NumericLimits<idx_t>::Maximum()) {
	D_ASSERT(read_ahead_depth_p > 0);
	executor = make_uniq<TaskExecutor>(context, TaskSchedulerType::ASYNC);
}

idx_t MultiFileReadAhead::ResolveDepth(ClientContext &context, idx_t max_threads) {
	auto configured_depth = Settings::Get<ReadAheadDepthSetting>(context);
	if (configured_depth == -1) {
		return MaxValue<idx_t>(max_threads / 4, 4);
	}
	return NumericCast<idx_t>(configured_depth);
}

MultiFileReadAhead::~MultiFileReadAhead() {
	Drain();
}

void MultiFileReadAhead::SetDone() {
	done = true;
}

bool MultiFileReadAhead::IsDone() const {
	return done.load();
}

bool MultiFileReadAhead::TryReserveSlot() {
	if (pending_io_bytes.load() >= io_byte_budget) {
		return false;
	}
	if (active_jobs.fetch_add(1) >= read_ahead_depth) {
		active_jobs--;
		return false;
	}
	active_producers++;
	return true;
}

void MultiFileReadAhead::AbortProduce() {
	ReleaseSlot();
	active_producers--;
}

bool MultiFileReadAhead::HasActiveProducers() const {
	return active_producers.load() > 0;
}

void MultiFileReadAhead::PushJob(unique_ptr<MultiFileScanJob> job, vector<unique_ptr<AsyncTask>> io_tasks) {
	auto pending = make_shared_ptr<atomic<idx_t>>(io_tasks.size());
	job->io_tasks_pending = pending;
	for (auto &task : io_tasks) {
		job->io_bytes += task->GetIOSize();
	}
	pending_io_bytes += job->io_bytes;
	// schedule the reads detached on the async pool right away
	for (auto &task : io_tasks) {
		executor->ScheduleTask(make_uniq<ReadAheadIOTask>(*executor, std::move(task), pending));
	}
	{
		lock_guard<mutex> guard(lock);
		// producers push concurrently, so admit jobs to the queue in batch-index order
		pending_jobs.emplace(job->batch_index, std::move(job));
		while (!pending_jobs.empty() && pending_jobs.begin()->first == next_batch_index) {
			ready_queue.push_back(std::move(pending_jobs.begin()->second));
			pending_jobs.erase(pending_jobs.begin());
			next_batch_index++;
		}
	}
	active_producers--;
}

unique_ptr<MultiFileScanJob> MultiFileReadAhead::ClaimJob() {
	lock_guard<mutex> guard(lock);
	if (ready_queue.empty()) {
		return nullptr;
	}
	auto job = std::move(ready_queue.front());
	ready_queue.pop_front();
	ReleaseSlot();
	pending_io_bytes -= job->io_bytes;
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
	if (job.io_tasks_pending) {
		auto &pending = *job.io_tasks_pending;
		while (pending.load() > 0) {
			shared_ptr<Task> task;
			if (executor->GetTask(task)) {
				// pull a queued I/O task off the executor and run it on this thread
				task->Execute(TaskExecutionMode::PROCESS_ALL);
				task.reset();
			} else {
				TaskScheduler::YieldThread();
			}
		}
	}
	if (executor->HasError()) {
		executor->ThrowError();
	}
}

void MultiFileReadAhead::ReleaseSlot() {
	D_ASSERT(active_jobs.load() > 0);
	active_jobs--;
}

void MultiFileReadAhead::Drain() noexcept {
	try {
		// cancel I/O that has not started yet
		executor->PushError(ErrorData(ExceptionType::INTERRUPT, "read-ahead scan was torn down"));
		executor->WorkOnTasks();
	} catch (...) { // LCOV_EXCL_START
	}               // LCOV_EXCL_STOP
}

MultiFileGlobalState::~MultiFileGlobalState() = default;

} // namespace duckdb
