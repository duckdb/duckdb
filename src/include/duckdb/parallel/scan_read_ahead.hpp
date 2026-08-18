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
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/serializer/async_memory_governor.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <functional>

namespace duckdb {
class ClientContext;
struct TableFunctionInput;
struct LocalTableFunctionState;

class ReadAheadJobCompletion {
public:
	ReadAheadJobCompletion(shared_ptr<TaskExecutor> executor_p, idx_t io_task_count)
	    : executor(std::move(executor_p)), pending_io_tasks(io_task_count) {
	}

public:
	//! Mark one I/O task as completed, waking the scan task when it was the last one
	void FinishIOTask();
	//! Try to park the calling scan task until the job's I/O completes, the last I/O task to finish wakes it.
	bool TryPark(const InterruptState &interrupt_state);
	//! Try to park the scan task through the table function's interrupt state, setting BLOCKED on success
	bool TryPark(TableFunctionInput &data_p);
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

//! Base of the job types driven by ScanReadAhead
struct ScanReadAheadJob {
	ScanReadAheadJob() = default;
	ScanReadAheadJob(ScanReadAheadJob &&) noexcept = default;
	ScanReadAheadJob &operator=(ScanReadAheadJob &&) noexcept = default;
	virtual ~ScanReadAheadJob();

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

	//! Batch index of this job, drives ordered queue admission
	idx_t batch_index = 0;
	//! Completion of the job's scheduled I/O
	shared_ptr<ReadAheadJobCompletion> io_completion;
	//! Total bytes of scheduled I/O for this job
	idx_t io_bytes = 0;
};

//! Outcome of ScanReadAhead::AcquireJob
enum class ScanReadAheadAcquire : uint8_t {
	ACQUIRED,  //! a job with settled I/O is now held
	EXHAUSTED, //! every job has been produced and claimed, the scan is done
	PARKED     //! the scan task parked until the claimed job's I/O completes, the caller must yield
};

//! Throw when the query was interrupted, otherwise yield the thread
void ScanReadAheadYield(ClientContext &context);

//! Resolve the read_ahead_depth setting, returns false when read-ahead is disabled.
//! An invalid depth means automatic mode, its interpretation is up to the scan.
bool TryGetReadAheadDepth(ClientContext &context, optional_idx &depth);

//! Drives read-ahead for a scan, its purpose is to keep several scan jobs scheduled ahead of decoding.
//! Jobs derive from ScanReadAheadJob, callers claim them back with Cast.
class ScanReadAhead {
public:
	ScanReadAhead(ClientContext &context, idx_t read_ahead_depth_p,
	              unique_ptr<ManagedAsyncMemoryGovernor> memory_governor_p);
	~ScanReadAhead();

	//! Create the read-ahead driver from the read_ahead_depth setting, returns null when read-ahead is disabled.
	//! -1 means automatic mode, unlimited depth with the backlog bounded by a temp-memory reservation.
	static unique_ptr<ScanReadAhead> Create(ClientContext &context);

public:
	//! Claims the next job and schedules its I/O, filling io_tasks when the I/O was detached to the pool.
	//! Returns null when there are no more jobs to produce.
	using ProduceJobCallback = std::function<unique_ptr<ScanReadAheadJob>(vector<unique_ptr<AsyncTask>> &io_tasks)>;

	//! Try to produce one job into the queue.
	bool TryProduceJob(const ProduceJobCallback &claim_and_schedule);
	//! Check if scan is done, i.e., no more jobs to do
	bool IsDone() const;
	//! Whether any thread holds a reserved slot it has not pushed a job for yet
	bool HasActiveProducers() const;
	//! Pop the oldest queued job
	unique_ptr<ScanReadAheadJob> ClaimJob();
	//! Push a finished job's scan state, so learned scan state carries over to jobs created later
	void PushState(unique_ptr<LocalTableFunctionState> state);
	//! Pop a recycled scan state, returns null when none is available
	unique_ptr<LocalTableFunctionState> TryPopState();
	//! Block until the claimed job's scheduled I/O has completed
	void WaitForJob(ScanReadAheadJob &job);
	//! Acquire the next job, producing and claiming as needed; on ACQUIRED the job's I/O has been settled.
	//! On PARKED the caller must yield holding the claimed job, the next call settles its completed I/O.
	ScanReadAheadAcquire AcquireJob(ClientContext &context, TableFunctionInput &data_p,
	                                const ProduceJobCallback &claim_and_schedule, unique_ptr<ScanReadAheadJob> &job);

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
	void SetDone();
	//! Reserve an in-flight job slot for producing a job
	bool TryReserveSlot();
	//! Schedule the job's I/O and admit the job to the queue
	void PushJob(unique_ptr<ScanReadAheadJob> job, vector<unique_ptr<AsyncTask>> io_tasks);
	//! Push an error onto the async executor
	void PushError(ErrorData error);
	//! Throw if any read-ahead thread or task pushed an error
	void ThrowIfError();
	//! Release a read-ahead slot
	void ReleaseSlot();

private:
	//! Maximum number of jobs scheduled ahead of decoding, unlimited in the -1 auto mode
	const idx_t read_ahead_depth;
	//! Async memory governor
	unique_ptr<ManagedAsyncMemoryGovernor> memory_governor;
	//! Backlog budget granted by the reservation, refreshed whenever a job is pushed
	atomic<idx_t> backlog_budget {0};

	mutable mutex lock;
	deque<unique_ptr<ScanReadAheadJob>> ready_queue;
	//! Jobs pushed out of order, held back until all earlier batch indexes are admitted to the queue
	map<idx_t, unique_ptr<ScanReadAheadJob>> pending_jobs;
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
	//! Async I/O executor (async pool)
	shared_ptr<TaskExecutor> executor;
};

} // namespace duckdb
