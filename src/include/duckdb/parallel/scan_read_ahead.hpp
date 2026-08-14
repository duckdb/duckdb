//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/scan_read_ahead.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/serializer/async_memory_governor.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <functional>

namespace duckdb {
class ClientContext;

class ReadAheadJobCompletion {
public:
	ReadAheadJobCompletion(shared_ptr<TaskExecutor> executor_p, idx_t io_task_count)
	    : executor(std::move(executor_p)), pending_io_tasks(io_task_count) {
	}

public:
	//! Number of I/O tasks that have not completed yet
	idx_t PendingIOTasks() const {
		return pending_io_tasks.load();
	}
	//! Mark one I/O task as completed, waking the scan task when it was the last one
	void FinishIOTask();
	//! Try to park the calling scan task until the job's I/O completes, the last I/O task to finish wakes it.
	bool TryPark(const InterruptState &interrupt_state);
	//! Block until the job's I/O completed, running scheduled I/O inline so progress does not depend on pool threads
	void WaitForIO();

private:
	//! Executor holding the scheduled I/O, waiters drain it inline
	shared_ptr<TaskExecutor> executor;
	atomic<idx_t> pending_io_tasks;
	//! Holds the scan task parked on this job's I/O
	StateWithBlockableTasks parked_scan;
};

//! Async task that runs one scan job's I/O and releases the job's pending count when done.
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

//! Drives read-ahead for a scan, its purpose is to keep several scan jobs scheduled ahead of decoding.
//! JOB must expose batch_index, io_completion and io_bytes fields, STATE is the recyclable per-job scan state.
template <class JOB, class STATE>
class ScanReadAhead {
public:
	ScanReadAhead(ClientContext &context, idx_t read_ahead_depth_p,
	              unique_ptr<ManagedAsyncMemoryGovernor> memory_governor_p)
	    : read_ahead_depth(read_ahead_depth_p), memory_governor(std::move(memory_governor_p)) {
		D_ASSERT(read_ahead_depth_p > 0);
		backlog_budget = memory_governor ? memory_governor->BackpressureBudget() : NumericLimits<idx_t>::Maximum();
		executor = make_shared_ptr<TaskExecutor>(context, TaskSchedulerType::ASYNC);
	}
	~ScanReadAhead() {
		executor->CancelAndDrain();
	}

public:
	//! Claims the next job and schedules its I/O, filling io_tasks when the I/O was detached to the pool.
	using ProduceJobCallback = std::function<bool(JOB &job, vector<unique_ptr<AsyncTask>> &io_tasks)>;

	//! Try to produce one job into the queue.
	bool TryProduceJob(const ProduceJobCallback &claim_and_schedule) {
		ThrowIfError();
		if (IsDone() || !TryReserveSlot()) {
			return false;
		}
		ProducerReservation reservation(*this);
		try {
			auto job = make_uniq<JOB>();
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

	//! Check if scan is done, i.e., no more jobs to do
	bool IsDone() const {
		return done.load();
	}

	//! Whether any thread holds a reserved slot it has not pushed a job for yet
	bool HasActiveProducers() const {
		return active_producers.load() > 0;
	}

	//! Pop the oldest queued job
	unique_ptr<JOB> ClaimJob() {
		lock_guard<mutex> guard(lock);
		if (ready_queue.empty()) {
			return nullptr;
		}
		auto job = std::move(ready_queue.front());
		ready_queue.pop_front();
		ReleaseSlot();
		return job;
	}

	//! Push a finished job's scan state, so learned scan state carries over to jobs created later
	void PushState(unique_ptr<STATE> state) {
		D_ASSERT(state);
		lock_guard<mutex> guard(lock);
		state_pool.push_back(std::move(state));
	}

	//! Pop a recycled scan state, returns null when none is available
	unique_ptr<STATE> TryPopState() {
		lock_guard<mutex> guard(lock);
		if (state_pool.empty()) {
			return nullptr;
		}
		auto state = std::move(state_pool.back());
		state_pool.pop_back();
		return state;
	}

	//! Block until the claimed job's scheduled I/O has completed
	void WaitForJob(JOB &job) {
		if (job.io_completion) {
			job.io_completion->WaitForIO();
		}
		// the job's I/O has completed, release its budget charge
		pending_io_bytes -= job.io_bytes;
		job.io_bytes = 0;
		ThrowIfError();
	}

private:
	//! Settles the reservation taken by TryReserveSlot
	struct ProducerReservation {
		explicit ProducerReservation(ScanReadAhead &read_ahead) : read_ahead(read_ahead) {
		}
		~ProducerReservation() {
			if (!committed) {
				read_ahead.ReleaseSlot();
			}
			--read_ahead.active_producers;
		}

		ScanReadAhead &read_ahead;
		bool committed = false;
	};

	//! Mark the scan as done, i.e., no more jobs to produce
	void SetDone() {
		done = true;
	}

	//! Reserve an in-flight job slot for producing a job
	bool TryReserveSlot() {
		const bool over_budget = pending_io_bytes.load() >= backlog_budget.load();
		const idx_t depth = over_budget ? 1 : read_ahead_depth;
		if (active_jobs.fetch_add(1) >= depth) {
			--active_jobs;
			return false;
		}
		++active_producers;
		return true;
	}

	//! Schedule the job's I/O and admit the job to the queue
	void PushJob(unique_ptr<JOB> job, vector<unique_ptr<AsyncTask>> io_tasks) {
		// beyond its scheduled I/O a job carries scan-state overhead (row-group sized decode buffers)
		static constexpr idx_t MINIMUM_JOB_IO_CHARGE = 16ULL * 1024 * 1024;
		auto completion = make_shared_ptr<ReadAheadJobCompletion>(executor, io_tasks.size());
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

	//! Push an error onto the async executor
	void PushError(ErrorData error) {
		executor->PushError(std::move(error));
	}

	//! Throw if any read-ahead thread or task pushed an error
	void ThrowIfError() {
		if (executor->HasError()) {
			executor->ThrowError();
		}
	}

	//! Release a read-ahead slot
	void ReleaseSlot() {
		D_ASSERT(active_jobs.load() > 0);
		active_jobs--;
	}

private:
	//! Maximum number of jobs scheduled ahead of decoding, unlimited in the -1 auto mode
	const idx_t read_ahead_depth;
	//! Async memory governor
	unique_ptr<ManagedAsyncMemoryGovernor> memory_governor;
	//! Backlog budget granted by the reservation, refreshed whenever a job is pushed
	atomic<idx_t> backlog_budget {0};

	mutable mutex lock;
	deque<unique_ptr<JOB>> ready_queue;
	//! Jobs pushed out of order, held back until all earlier batch indexes are admitted to the queue
	map<idx_t, unique_ptr<JOB>> pending_jobs;
	//! The batch index the queue admits next
	idx_t next_batch_index = 0;
	//! Scan states of finished jobs
	vector<unique_ptr<STATE>> state_pool;
	//! Jobs scheduled ahead of decoding
	atomic<idx_t> active_jobs {0};
	//! Bytes of scheduled I/O that has not completed yet, released once the claimed job's I/O finished
	atomic<idx_t> pending_io_bytes {0};
	atomic<bool> done {false};
	//! Threads that reserved a slot but have not pushed their job yet
	atomic<idx_t> active_producers {0};
	//! Async I/O executor (async pool)
	shared_ptr<TaskExecutor> executor;
};

} // namespace duckdb
