//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/async_write_queue.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/query_context.hpp"

namespace duckdb {

class ClientContext;
class TaskExecutor;
class TemporaryMemoryState;

//! Owned payload that can be handed to the async write queue.
class AsyncWritePayload {
public:
	virtual ~AsyncWritePayload() = default;

	//! Pointer to the bytes to write. The buffer must remain valid for the lifetime of this object.
	virtual data_ptr_t Ptr() = 0;
	//! Number of bytes exposed by Ptr().
	virtual idx_t Size() const = 0;
};

//! Compatibility name for existing stream-oriented callers.
using AsyncWriteBuffer = AsyncWritePayload;

//! Physical target used by AsyncWriteQueue after logical offsets have already been assigned by the caller.
class AsyncWriteTarget {
public:
	virtual ~AsyncWriteTarget() = default;

	//! Whether independent writes to different offsets can safely run concurrently.
	virtual bool SupportsPositionalWrites() = 0;
	//! Write a specific byte range using the target's positional write path.
	virtual void Write(QueryContext context, data_ptr_t buffer, idx_t size, idx_t offset) = 0;
	//! Write bytes using the target's sequential write path.
	virtual void Write(QueryContext context, data_ptr_t buffer, idx_t size) = 0;
};

//! Shared async write scheduler for targets that register owned payloads with pre-assigned logical offsets.
//! Callers are responsible for assigning offsets and externally serializing RegisterWrite calls.
class AsyncWriteQueue {
	friend class AsyncWriteQueueTask;
	friend class AsyncWriteQueueDrainTaskGuard;

public:
	//! Queue tuning derived by the caller from its target and workload.
	struct Options {
		//! Maximum contiguous small-write range to merge into one physical write.
		idx_t coalesce_threshold = 0;
		//! Minimum unscheduled bytes before threshold scheduling starts the first async task.
		idx_t first_task_schedule_threshold = 0;
		//! Minimum TemporaryMemoryManager reservation while writes are outstanding.
		idx_t min_pending_bytes = 0;
		//! Hard cap over the TemporaryMemoryManager reservation.
		idx_t max_pending_bytes = 0;
		//! Maximum bytes one async drain task should take before yielding scheduler capacity.
		idx_t drain_task_byte_budget = 0;
		//! Maximum scheduled/running drain tasks when positional writes are supported.
		idx_t max_active_drain_tasks = 1;
		//! Stop each coalesced write at coalesce_threshold instead of building larger remote-style chunks.
		bool limit_coalesced_write_size = false;
	};

	//! Whether registering a payload may schedule an async drain task immediately.
	enum class ScheduleMode : uint8_t { ALLOW, DEFER };
	//! Whether to force a TemporaryMemoryState growth check while a registration batch is open.
	enum class MemoryUpdateMode : uint8_t { COARSE, FORCE };
	//! Whether to schedule only enough task capacity for normal overlap, or force all pending bytes to drain.
	enum class SchedulePolicy : uint8_t { THRESHOLD, FORCE };
	//! Whether async drain tasks can write independent target ranges concurrently.
	enum class DrainMode : uint8_t { SEQUENTIAL, POSITIONAL };
	//! Whether waiting for scheduled writes should preserve an open registration batch.
	enum class BatchDrainMode : uint8_t { PRESERVE_BATCH, FORCE_CLOSE_BATCH };
	//! Whether task estimation should include the final under-budget tail.
	enum class PendingTaskCountMode : uint8_t { FULL_BUDGET_ONLY, INCLUDE_TAIL };

public:
	DUCKDB_API AsyncWriteQueue(QueryContext context, ClientContext &client_context, AsyncWriteTarget &target,
	                           Options options, idx_t async_threads);
	DUCKDB_API ~AsyncWriteQueue();

	AsyncWriteQueue(const AsyncWriteQueue &) = delete;
	AsyncWriteQueue &operator=(const AsyncWriteQueue &) = delete;

public:
	//! Return whether writes are drained by async scheduler tasks. If false, RegisterWrite writes synchronously.
	DUCKDB_API bool IsAsync() const;
	//! Return whether the async task executor has captured an error.
	DUCKDB_API bool HasError();
	//! Add an owned payload with its assigned target offset to the configured sync/async write path.
	DUCKDB_API void RegisterWrite(unique_ptr<AsyncWritePayload> payload, idx_t offset,
	                              ScheduleMode schedule_mode = ScheduleMode::ALLOW);
	//! Enter a registration batch, delaying async draining until the batch is left.
	DUCKDB_API void BeginBatch();
	//! Leave a registration batch without scheduling, blocking, or throwing.
	DUCKDB_API void LeaveBatch() noexcept;
	//! Return whether a registration batch is currently open.
	DUCKDB_API bool HasOpenBatch();
	//! Schedule as many drain tasks as the pending queue allows.
	DUCKDB_API void SchedulePendingWrites(SchedulePolicy policy = SchedulePolicy::THRESHOLD);
	//! Help drain async writes when pending bytes exceed the current memory budget. No-op while a batch is open.
	DUCKDB_API void ApplyBackpressure();
	//! Wait for scheduled writes, optionally restoring an active registration batch afterwards.
	DUCKDB_API void WaitAll(BatchDrainMode batch_drain_mode = BatchDrainMode::PRESERVE_BATCH);
	//! Reset the next expected offset after all registered writes have drained.
	DUCKDB_API void ResetNextOffset(idx_t offset);
	//! Release the queue's TemporaryMemoryState reservation.
	DUCKDB_API void ReleaseMemoryReservation();
	//! Surface an error thrown by an async drain task.
	DUCKDB_API void RethrowTaskError();

private:
	struct PendingWrite {
		PendingWrite(unique_ptr<AsyncWritePayload> payload, idx_t offset);

		idx_t Size() const;

		unique_ptr<AsyncWritePayload> payload;
		idx_t offset;
	};

private:
	//! Schedule drain tasks from already registered pending writes. Safe to call from async drain tasks.
	void SchedulePendingWritesInternal(SchedulePolicy policy = SchedulePolicy::THRESHOLD);
	//! Grow the TemporaryMemoryState reservation coarsely; it is released only when the queue closes.
	void UpdateMemoryState(MemoryUpdateMode mode = MemoryUpdateMode::COARSE);

	//! Return the current async backlog budget after applying the fixed queue cap.
	idx_t BackpressureBudget();
	//! Effective byte budget for one drain task, never smaller than the coalescing threshold.
	idx_t DrainTaskByteBudget() const;
	//! Return queued/in-flight bytes that have not reached the target yet. Caller must hold lock.
	idx_t TotalPendingBytes() const;
	//! Select the pending write range one drain task would claim. Caller must hold lock.
	idx_t SelectPendingWriteEnd(idx_t start, idx_t &selected_bytes) const;
	//! Skip ranges already covered by scheduled-but-not-started drain tasks. Caller must hold lock.
	idx_t FirstUnscheduledPendingWrite(idx_t &scheduled_bytes) const;
	//! Count how many useful drain tasks remain after start_write. Caller must hold lock.
	idx_t CountPendingWriteTasks(idx_t start_write, idx_t available_slots, PendingTaskCountMode mode) const;
	//! Estimate how many drain tasks are useful for the currently queued writes. Caller must hold lock.
	idx_t EstimateScheduleCount(idx_t available_slots, SchedulePolicy policy) const;

	//! Move one byte-budgeted prefix of pending writes into a drain task. Caller owns a scheduled task slot.
	idx_t TakePendingWrites(vector<PendingWrite> &writes);
	//! Release one scheduled/running drain task slot and its in-flight byte accounting.
	void FinishDrainTask(idx_t in_flight_task_bytes);
	//! Release a task slot for a scheduled task that never entered the queue because another task failed.
	void CancelScheduledDrainTask();
	//! Release multiple reserved task slots that were never scheduled.
	void CancelScheduledDrainTasks(idx_t task_count);

	//! Async task entry point that drains one budgeted batch of pending payloads.
	void DrainPendingWrites();
	//! Write pending payloads in registration order, coalescing adjacent small writes.
	idx_t WritePendingWrites(vector<PendingWrite> &writes);
	//! Write bytes to the target at the assigned logical offset.
	void WriteBuffer(data_ptr_t buffer, idx_t size, idx_t offset);
	//! Validate a new registration against the contiguous offset contract.
	idx_t ValidateRegistrationOffset(idx_t offset, idx_t write_size) const;
	//! Validate a pending write before coalescing it with its predecessor.
	void VerifyContiguousWrite(const PendingWrite &write, idx_t expected_offset) const;
	//! Return offset + write_size, throwing if it overflows idx_t.
	idx_t NextWriteOffset(idx_t offset, idx_t write_size) const;
	//! Throw if the queue still owns registered or scheduled write work.
	void VerifyDrained() const;
	//! Execute one queued drain task owned by this queue, or yield if the tasks are already running.
	void WorkOnSingleTask();

private:
	QueryContext context;
	ClientContext &client_context;
	AsyncWriteTarget &target;
	Options options;

	//! Temporary memory reservation state used to limit queued async write data.
	unique_ptr<TemporaryMemoryState> memory_state;
	//! Last remaining-size request sent to TemporaryMemoryManager. Grows monotonically until close.
	idx_t memory_request_bytes = 0;
	//! Whether async tasks may drain independent ranges concurrently using positional writes.
	DrainMode drain_mode = DrainMode::SEQUENTIAL;
	//! Maximum number of scheduled/running drain tasks for this queue.
	idx_t max_active_drain_tasks = 1;

	//! Protects state shared between the registering thread and async drain tasks.
	mutex lock;
	//! Pending payloads in registration order with pre-assigned logical offsets.
	vector<PendingWrite> pending_writes;
	//! Bytes queued in pending_writes that have not been taken by a drain task yet.
	idx_t pending_bytes = 0;
	//! Bytes owned by drain tasks that have not reached the target yet.
	idx_t in_flight_bytes = 0;
	//! Nested batch depth. While non-zero, async draining and backpressure are delayed.
	idx_t batch_depth = 0;
	//! Scheduled or running drain tasks for this queue.
	idx_t active_drain_tasks = 0;
	//! Scheduled drain tasks that have not yet claimed pending bytes.
	idx_t pending_drain_tasks = 0;
	//! Next logical offset expected by RegisterWrite. Enforces v1 contiguous-registration semantics.
	idx_t next_registration_offset = 0;

	//! Async task executor. If absent, writes are performed synchronously on registration.
	//! Keep this after task-accounting fields so queued task destructors can still release slots.
	unique_ptr<TaskExecutor> executor;
};

} // namespace duckdb
