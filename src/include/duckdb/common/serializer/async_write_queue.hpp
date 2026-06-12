//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/async_write_queue.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include <functional>

namespace duckdb {

class ClientContext;
class TaskExecutor;
class TemporaryMemoryState;

//! Owned payload that can be handed to an async write queue.
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

//! Completion callback for one physical async write request. The error is set when the write failed.
using AsyncWriteCompletionCallback = std::function<void(idx_t offset, idx_t size, optional_ptr<const ErrorData> error)>;

//! One positional physical write request.
class AsyncWriteRequest {
public:
	AsyncWriteRequest() = default;
	AsyncWriteRequest(unique_ptr<AsyncWritePayload> payload, idx_t offset,
	                  AsyncWriteCompletionCallback completion = nullptr);

	idx_t Size() const;

	unique_ptr<AsyncWritePayload> payload;
	idx_t offset = 0;
	AsyncWriteCompletionCallback completion;
};

//! Positional physical write target used by the low-level AsyncWriteQueue.
class AsyncWriteTarget {
public:
	virtual ~AsyncWriteTarget() = default;

	//! Write a specific byte range using the target's positional write path.
	virtual void Write(data_ptr_t buffer, idx_t size, idx_t offset) = 0;
};

//! Minimal positional async write scheduler.
//! Requests are independent positional writes; stream ordering, coalescing, and memory policy live in wrappers.
class AsyncWriteQueue {
	friend class AsyncWriteQueueTask;
	friend class AsyncWriteQueueTaskGuard;

public:
	struct Options {
		//! Maximum scheduled/running write tasks for this queue.
		idx_t max_active_tasks = 1;
	};

public:
	DUCKDB_API AsyncWriteQueue(ClientContext &client_context, AsyncWriteTarget &target, Options options,
	                           idx_t async_threads);
	DUCKDB_API ~AsyncWriteQueue();

	AsyncWriteQueue(const AsyncWriteQueue &) = delete;
	AsyncWriteQueue &operator=(const AsyncWriteQueue &) = delete;

public:
	//! Return whether writes are drained by async scheduler tasks. If false, Submit writes synchronously.
	DUCKDB_API bool IsAsync() const;
	//! Return whether the async task executor has captured an error.
	DUCKDB_API bool HasError();
	//! Submit one owned positional request to the configured sync/async write path.
	DUCKDB_API void Submit(AsyncWriteRequest request);
	//! Return queued/in-flight bytes that have not reached the target yet.
	DUCKDB_API idx_t PendingBytes();
	//! Execute one queued task owned by this queue, or yield if the tasks are already running.
	DUCKDB_API void WorkOnPendingTask();
	//! Wait until all submitted writes have reached the target.
	DUCKDB_API void Flush();
	//! Wait for all writes and close the queue.
	DUCKDB_API void Close();
	//! Surface an error thrown by an async drain task.
	DUCKDB_API void RethrowTaskError();

private:
	struct PendingRequest {
		PendingRequest() = default;
		explicit PendingRequest(AsyncWriteRequest request);

		idx_t Size() const;

		AsyncWriteRequest request;
		idx_t size;
	};

private:
	//! Schedule pending requests until max_active_tasks is reached.
	void ScheduleTasksInternal();
	//! Move one pending request into a write task. Caller owns a scheduled task slot.
	bool TakeRequest(PendingRequest &request);
	//! Release one scheduled/running task slot and its in-flight byte accounting.
	void FinishTask(idx_t request_size);
	//! Release a task slot for a scheduled task that never entered the queue because another task failed.
	void CancelScheduledTask();
	//! Release multiple reserved task slots that were never scheduled.
	void CancelScheduledTasks(idx_t task_count);

	//! Async task entry point that drains one positional request.
	void DrainRequest();
	//! Write request bytes to the target and invoke its completion callback.
	void WriteRequest(AsyncWriteRequest request);
	//! Invoke a completion callback outside the queue lock.
	void CompleteRequest(AsyncWriteRequest &request, idx_t size, optional_ptr<const ErrorData> error);
	//! Write bytes to the target at the assigned physical offset.
	void WriteBuffer(data_ptr_t buffer, idx_t size, idx_t offset);
	//! Throw if a mutating API is used after Close().
	void VerifyOpen() const;
	//! Throw if the queue still owns registered or scheduled write work.
	void VerifyDrained() const;
	//! Discard queued requests after an async write failure once all scheduled tasks have stopped.
	void CancelPendingRequestsAfterFailure() noexcept;

private:
	ClientContext &client_context;
	AsyncWriteTarget &target;
	Options options;

	//! Protects state shared between the submitting thread and async write tasks.
	mutex lock;
	//! Positional requests waiting for an async task.
	deque<PendingRequest> pending_requests;
	//! Bytes queued in pending_requests that have not been taken by a task yet.
	idx_t pending_bytes = 0;
	//! Bytes owned by write tasks that have not reached the target yet.
	idx_t in_flight_bytes = 0;
	//! Scheduled or running write tasks for this queue.
	idx_t active_tasks = 0;
	//! Scheduled write tasks that have not yet claimed a request.
	idx_t pending_tasks = 0;
	//! Set after Close() has drained the queue. Further submissions are rejected.
	bool closed = false;

	//! Async task executor. If absent, writes are performed synchronously on submission.
	//! Keep this after task-accounting fields so queued task destructors can still release slots.
	unique_ptr<TaskExecutor> executor;
};

//! Stream target used by ManagedAsyncWriteQueue. Sequential fallback is handled here, not in AsyncWriteQueue.
class ManagedAsyncWriteTarget {
public:
	virtual ~ManagedAsyncWriteTarget() = default;

	//! Whether contiguous registered writes can safely drain concurrently through positional writes.
	virtual bool SupportsPositionalWrites() = 0;
	//! Write a specific byte range using the target's positional write path.
	virtual void Write(data_ptr_t buffer, idx_t size, idx_t offset) = 0;
	//! Write bytes using the target's sequential write path.
	virtual void Write(data_ptr_t buffer, idx_t size) = 0;
};

//! Managed stream-oriented write queue built on top of AsyncWriteQueue.
//! V1 is a contiguous logical write queue: each RegisterWrite offset must match the next expected offset.
//! Callers are responsible for assigning offsets and externally serializing RegisterWrite calls.
class ManagedAsyncWriteQueue : private AsyncWriteTarget {
public:
	//! Queue tuning derived by the caller from its target and workload.
	//! Defaults are minimal: no coalescing, no TMM backpressure, immediate first-task scheduling.
	struct Options {
		//! Stream coalescing policy. Zero disables coalescing and writes each payload directly.
		idx_t coalesce_threshold = 0;
		//! Generic scheduling policy: minimum unscheduled bytes before threshold scheduling starts the first task.
		idx_t first_task_schedule_threshold = 0;
		//! Generic memory policy: minimum TemporaryMemoryManager reservation while writes are outstanding.
		idx_t min_pending_bytes = 0;
		//! Generic memory policy: hard cap over the TemporaryMemoryManager reservation. Zero disables backpressure.
		idx_t max_pending_bytes = 0;
		//! Generic scheduling policy: maximum bytes one managed drain request should take.
		idx_t drain_task_byte_budget = 0;
		//! Generic scheduling policy: maximum submitted/running requests when positional writes are supported.
		idx_t max_active_drain_tasks = 1;
		//! Stream coalescing policy: stop each coalesced write at coalesce_threshold.
		bool limit_coalesced_write_size = false;
	};

	//! Whether registering a payload may schedule an async drain request immediately.
	enum class ScheduleMode : uint8_t { ALLOW, DEFER };
	//! Whether to force a TemporaryMemoryState growth check while a registration batch is open.
	enum class MemoryUpdateMode : uint8_t { COARSE, FORCE };
	//! Whether to schedule only enough request capacity for normal overlap, or force all pending bytes to drain.
	enum class SchedulePolicy : uint8_t { THRESHOLD, FORCE };
	//! Whether async requests can write independent target ranges concurrently.
	enum class DrainMode : uint8_t { SEQUENTIAL, POSITIONAL };
	//! Whether waiting for scheduled writes should preserve an open registration batch.
	enum class BatchDrainMode : uint8_t { PRESERVE_BATCH, FORCE_CLOSE_BATCH };
	//! Whether task estimation should include the final under-budget tail.
	enum class PendingTaskCountMode : uint8_t { FULL_BUDGET_ONLY, INCLUDE_TAIL };

public:
	DUCKDB_API ManagedAsyncWriteQueue(ClientContext &client_context, ManagedAsyncWriteTarget &target, Options options,
	                                  idx_t async_threads);
	DUCKDB_API ~ManagedAsyncWriteQueue();

	ManagedAsyncWriteQueue(const ManagedAsyncWriteQueue &) = delete;
	ManagedAsyncWriteQueue &operator=(const ManagedAsyncWriteQueue &) = delete;

public:
	//! Return whether writes are drained by async scheduler tasks. If false, RegisterWrite writes synchronously.
	DUCKDB_API bool IsAsync() const;
	//! Return whether the async task executor has captured an error.
	DUCKDB_API bool HasError();
	//! Add an owned payload at the next contiguous logical offset to the configured sync/async write path.
	DUCKDB_API void RegisterWrite(unique_ptr<AsyncWritePayload> payload, idx_t offset,
	                              ScheduleMode schedule_mode = ScheduleMode::ALLOW);
	//! Enter a registration batch, delaying async draining until the batch is left.
	DUCKDB_API void BeginBatch();
	//! Leave a registration batch without scheduling, blocking, or throwing.
	DUCKDB_API void LeaveBatch() noexcept;
	//! Return whether a registration batch is currently open.
	DUCKDB_API bool HasOpenBatch();
	//! Schedule as many drain requests as the pending queue allows.
	DUCKDB_API void SchedulePendingWrites(SchedulePolicy policy = SchedulePolicy::THRESHOLD);
	//! Help drain async writes when pending bytes exceed the current memory budget. No-op while a batch is open.
	DUCKDB_API void ApplyBackpressure();
	//! Wait for scheduled writes, optionally restoring an active registration batch afterwards.
	DUCKDB_API void WaitAll(BatchDrainMode batch_drain_mode = BatchDrainMode::PRESERVE_BATCH);
	//! Drain all writes, close any open registration batch, and release the TemporaryMemoryState reservation.
	DUCKDB_API void Close();
	//! Reset the next expected contiguous offset after all registered writes have drained.
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

	class CoalescedWritePayload;

private:
	//! Schedule drain requests from already registered pending writes.
	void SchedulePendingWritesInternal(SchedulePolicy policy = SchedulePolicy::THRESHOLD);
	//! Grow the TemporaryMemoryState reservation coarsely; it is released only when the queue closes.
	void UpdateMemoryState(MemoryUpdateMode mode = MemoryUpdateMode::COARSE);

	//! Return the current async backlog budget after applying the fixed queue cap.
	idx_t BackpressureBudget();
	//! Effective byte budget for one managed drain request, never smaller than the coalescing threshold.
	idx_t DrainTaskByteBudget() const;
	//! Return queued/submitted bytes that have not reached the target yet. Caller must hold lock.
	idx_t TotalPendingBytes() const;
	//! Select the pending write range one drain request would claim. Caller must hold lock.
	idx_t SelectPendingWriteEnd(idx_t start, idx_t &selected_bytes) const;
	//! Select the pending write range for the next physical write request. Caller must hold lock.
	idx_t SelectPhysicalWriteEnd(idx_t start, idx_t &selected_bytes) const;
	//! Count how many useful drain requests are pending. Caller must hold lock.
	idx_t CountPendingWriteTasks(idx_t start_write, idx_t available_slots, PendingTaskCountMode mode) const;
	//! Estimate how many drain requests are useful for the currently queued writes. Caller must hold lock.
	idx_t EstimateScheduleCount(idx_t available_slots, SchedulePolicy policy) const;

	//! Move one byte-budgeted prefix of pending writes into a physical async request.
	bool TakePendingWriteRequest(AsyncWriteRequest &request);
	//! Convert one or more contiguous pending writes into a lazily materialized payload.
	unique_ptr<AsyncWritePayload> CreatePayload(deque<PendingWrite> writes, idx_t size);
	//! Release byte accounting for one submitted physical request.
	void CompleteSubmittedWrite(idx_t offset, idx_t size, optional_ptr<const ErrorData> error);

	//! Validate a new registration against the contiguous offset contract.
	idx_t ValidateRegistrationOffset(idx_t offset, idx_t write_size) const;
	//! Throw if a mutating API is used after Close().
	void VerifyOpen() const;
	//! Validate a pending write before coalescing it with its predecessor.
	void VerifyContiguousWrite(const PendingWrite &write, idx_t expected_offset) const;
	//! Return offset + write_size, throwing if it overflows idx_t.
	idx_t NextWriteOffset(idx_t offset, idx_t write_size) const;
	//! Throw if the queue still owns registered or scheduled write work.
	void VerifyDrained() const;
	//! Discard queued writes after an async write failure once all submitted writes have stopped.
	void CancelPendingWritesAfterFailure() noexcept;

	//! Write bytes to the managed target at the assigned logical offset.
	void Write(data_ptr_t buffer, idx_t size, idx_t offset) override;

private:
	ClientContext &client_context;
	ManagedAsyncWriteTarget &target;
	Options options;

	//! Low-level positional request scheduler.
	unique_ptr<AsyncWriteQueue> write_queue;
	//! Temporary memory reservation state used to limit queued async write data.
	unique_ptr<TemporaryMemoryState> memory_state;
	//! Last remaining-size request sent to TemporaryMemoryManager. Grows monotonically until close.
	idx_t memory_request_bytes = 0;
	//! Whether async requests may drain independent ranges concurrently using positional writes.
	DrainMode drain_mode = DrainMode::SEQUENTIAL;
	//! Maximum number of submitted/running drain requests for this queue.
	idx_t max_active_drain_tasks = 1;

	//! Protects state shared between the registering thread and async completion callbacks.
	mutex lock;
	//! Pending payloads in registration order with pre-assigned logical offsets.
	deque<PendingWrite> pending_writes;
	//! Bytes queued in pending_writes that have not been submitted to AsyncWriteQueue yet.
	idx_t pending_bytes = 0;
	//! Bytes submitted to AsyncWriteQueue that have not completed yet.
	idx_t submitted_bytes = 0;
	//! Submitted physical requests that have not completed yet.
	idx_t submitted_requests = 0;
	//! Nested batch depth. While non-zero, async draining and backpressure are delayed.
	idx_t batch_depth = 0;
	//! Next logical offset expected by RegisterWrite. Enforces v1 contiguous-registration semantics.
	idx_t next_registration_offset = 0;
	//! Set after Close() has drained the queue. Further write registration is rejected.
	bool closed = false;
};

} // namespace duckdb
