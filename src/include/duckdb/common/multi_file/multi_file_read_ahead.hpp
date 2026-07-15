//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_read_ahead.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parallel/interrupt.hpp"

#include <functional>

namespace duckdb {
class ClientContext;
class ManagedAsyncMemoryGovernor;
class TaskExecutor;
class AsyncTask;
struct MultiFileScanJob;
struct LocalTableFunctionState;

class ReadAheadJobCompletion {
public:
	explicit ReadAheadJobCompletion(idx_t io_task_count) : pending_io_tasks(io_task_count) {
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

private:
	atomic<idx_t> pending_io_tasks;
	//! Holds the scan task parked on this job's I/O
	StateWithBlockableTasks parked_scan;
};

//! Drives read-ahead for the multi-file scan, it's purpose is to keep several scan jobs scheduled ahead of decoding
class MultiFileReadAhead {
public:
	MultiFileReadAhead(ClientContext &context, idx_t read_ahead_depth,
	                   unique_ptr<ManagedAsyncMemoryGovernor> memory_governor);
	~MultiFileReadAhead();

public:
	//! Create the read-ahead driver from the read_ahead_depth setting.
	//! -1 = automatic: unlimited depth, gated by a temp-memory reservation. Returns null when read-ahead is disabled.
	static unique_ptr<MultiFileReadAhead> Create(ClientContext &context);

	//! Claims the next job and schedules its I/O, filling io_tasks when the I/O was detached to the pool.
	using ProduceJobCallback = std::function<bool(MultiFileScanJob &job, vector<unique_ptr<AsyncTask>> &io_tasks)>;

	//! Try to produce one job into the queue.
	bool TryProduceJob(const ProduceJobCallback &claim_and_schedule);

	//! Check if scan is done, i.e., no more jobs to do
	bool IsDone() const;

	//! Whether any thread holds a reserved slot it has not pushed a job for yet
	bool HasActiveProducers() const;

	//! Pop the oldest queued job
	unique_ptr<MultiFileScanJob> ClaimJob();

	//! Push a finished job's scan state, so learned reader state carries over to jobs created later
	void PushState(unique_ptr<LocalTableFunctionState> state);

	//! Block until the claimed job's scheduled I/O has completed
	void WaitForJob(MultiFileScanJob &job);

private:
	//! RAII reservation produce attempts
	struct ProducerReservation;

	//! Mark the scan as done, i.e., no more jobs to produce
	void SetDone();
	//! Reserve an in-flight job slot for producing a job
	bool TryReserveSlot();
	//! Schedule the job's I/O and admit the job to the queue in batch-index order
	void PushJob(unique_ptr<MultiFileScanJob> job, vector<unique_ptr<AsyncTask>> io_tasks);
	//! Pop a recycled scan state, returns null when none is available
	unique_ptr<LocalTableFunctionState> TryPopState();
	//! Push an error onto the async executor
	void PushError(ErrorData error);
	//! Throw if any read-ahead thread or task pushed an error
	void ThrowIfError();
	//! Release a read-ahead slot
	void ReleaseSlot();

	//! Maximum number of jobs scheduled ahead of decoding, unlimited in the -1 auto mode
	const idx_t read_ahead_depth;
	//! Async memory governor
	unique_ptr<ManagedAsyncMemoryGovernor> memory_governor;
	//! Backlog budget granted by the reservation, refreshed whenever a job is pushed
	atomic<idx_t> backlog_budget {0};

	mutable mutex lock;
	deque<unique_ptr<MultiFileScanJob>> ready_queue;
	//! Jobs pushed out of order, held back until all earlier batch indexes are admitted to the queue
	map<idx_t, unique_ptr<MultiFileScanJob>> pending_jobs;
	//! The batch index the queue admits next
	idx_t next_batch_index = 0;
	//! Scan states of finished jobs
	vector<unique_ptr<LocalTableFunctionState>> state_pool;
	//! Jobs scheduled ahead of decoding
	atomic<idx_t> active_jobs {0};
	//! Bytes of scheduled I/O that has not completed yet, released once the claimed job's I/O finished
	atomic<idx_t> pending_io_bytes {0};
	atomic<bool> done {false};
	//! Threads that reserved a slot but have not pushed their job yet
	atomic<idx_t> active_producers {0};
	//! Async I/O executor (async pool).
	unique_ptr<TaskExecutor> executor;
};

} // namespace duckdb
