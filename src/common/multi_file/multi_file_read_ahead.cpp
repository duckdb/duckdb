#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"

#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

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
    : read_ahead_depth(read_ahead_depth_p) {
	D_ASSERT(read_ahead_depth_p > 0);
	executor = make_uniq<TaskExecutor>(context, TaskSchedulerType::ASYNC);
}

idx_t MultiFileReadAhead::ResolveDepth(ClientContext &context, idx_t max_threads) {
	auto configured_depth = Settings::Get<ReadAheadDepthSetting>(context);
	if (configured_depth < 0) {
		// TODO: We probably want to make this depend on a memory budget
		return max_threads + MaxValue<idx_t>(max_threads / 4, 4);
	}
	return NumericCast<idx_t>(configured_depth);
}

MultiFileReadAhead::~MultiFileReadAhead() {
	Drain();
}

idx_t MultiFileReadAhead::ActiveJobs() const {
	return active_jobs.load();
}

void MultiFileReadAhead::SetDone() {
	done = true;
}

bool MultiFileReadAhead::IsDone() const {
	return done.load();
}

void MultiFileReadAhead::PushJob(unique_ptr<MultiFileScanJob> job, vector<unique_ptr<AsyncTask>> io_tasks) {
	auto pending = make_shared_ptr<atomic<idx_t>>(io_tasks.size());
	job->io_tasks_pending = pending;
	// schedule the reads detached on the async pool
	for (auto &task : io_tasks) {
		executor->ScheduleTask(make_uniq<ReadAheadIOTask>(*executor, std::move(task), pending));
	}
	// bump active_jobs together with the enqueue so a throw above (before any state is published) cannot desync the
	// count
	lock_guard<mutex> guard(lock);
	active_jobs++;
	ready_queue.push_back(std::move(job));
}

unique_ptr<MultiFileScanJob> MultiFileReadAhead::ClaimJob() {
	lock_guard<mutex> guard(lock);
	if (ready_queue.empty()) {
		return nullptr;
	}
	auto job = std::move(ready_queue.front());
	ready_queue.pop_front();
	return job;
}

bool MultiFileReadAhead::TryBecomeProducer() {
	bool expected = false;
	return producing.compare_exchange_strong(expected, true);
}

void MultiFileReadAhead::EndProducer() {
	producing = false;
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

void MultiFileReadAhead::FinishJob() {
	D_ASSERT(active_jobs.load() > 0);
	active_jobs--;
}

void MultiFileReadAhead::Drain() noexcept {
	try {
		executor->WorkOnTasks();
	} catch (...) { // LCOV_EXCL_START
	} // LCOV_EXCL_STOP
}

MultiFileGlobalState::~MultiFileGlobalState() = default;

} // namespace duckdb
