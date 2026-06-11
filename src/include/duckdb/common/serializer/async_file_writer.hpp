//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/async_file_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/main/query_context.hpp"

namespace duckdb {

class ClientContext;
class CopiedAsyncWriteBuffer;
class TaskExecutor;
class TemporaryMemoryState;

class AsyncWriteBuffer {
public:
	virtual ~AsyncWriteBuffer() = default;

	//! Pointer to the bytes to write. The buffer must remain valid for the lifetime of this object.
	virtual data_ptr_t Ptr() = 0;
	//! Number of bytes exposed by Ptr().
	virtual idx_t Size() const = 0;
};

//! WriteStream implementation that registers writes cheaply and drains them on the async task scheduler.
//! This is a logical stream writer: offsets are assigned when writes are registered via GetTotalWritten().
//! Physical writes may complete out of order when positional writes are supported; WaitAll/Close complete the file.
//! Calls into this writer must be externally serialized; internal locking only coordinates with async drain tasks.
class AsyncFileWriter : public WriteStream {
	friend class AsyncFileWriterTask;
	friend class AsyncFileWriterDrainTaskGuard;

public:
	//! RAII handle that batches write registration. Async draining is delayed until the last guard is destroyed.
	class BatchGuard {
	public:
		BatchGuard(const BatchGuard &) = delete;
		BatchGuard &operator=(const BatchGuard &) = delete;
		DUCKDB_API BatchGuard(BatchGuard &&other) noexcept;
		BatchGuard &operator=(BatchGuard &&other) = delete;
		DUCKDB_API ~BatchGuard();

	private:
		friend class AsyncFileWriter;

		DUCKDB_API explicit BatchGuard(AsyncFileWriter &writer);

	private:
		optional_ptr<AsyncFileWriter> writer;
	};

	//! Default file-open behavior for creating a write-locked output file.
	static constexpr FileOpenFlags DEFAULT_OPEN_FLAGS = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;
	//! Capacity of the staging buffer used for small transient WriteData inputs.
	static constexpr idx_t DEFAULT_COPIED_BUFFER_CAPACITY = 4096;
	//! Local file systems are cheap to call, so only coalesce up to the buffered writer page size.
	static constexpr idx_t DEFAULT_LOCAL_COALESCE_THRESHOLD = 4096;
	//! Remote file systems benefit from fewer round trips, so coalesce contiguous small buffers more aggressively.
	static constexpr idx_t DEFAULT_REMOTE_COALESCE_THRESHOLD = 8ULL * 1024ULL * 1024ULL;
	//! Maximum queued async bytes retained per regular execution thread.
	static constexpr idx_t DEFAULT_MAX_PENDING_BYTES_PER_THREAD = 128ULL * 1024ULL * 1024ULL;
	//! Minimum async write reservation requested per regular execution thread.
	static constexpr idx_t DEFAULT_MIN_PENDING_BYTES_PER_THREAD = 8ULL * 1024ULL * 1024ULL;
	//! Maximum bytes a single async drain task should take before yielding scheduler capacity.
	static constexpr idx_t DEFAULT_DRAIN_TASK_BYTE_BUDGET = 32ULL * 1024ULL * 1024ULL;
	//! Local buffered writes rarely benefit from many concurrent pwrite calls to one file.
	static constexpr idx_t DEFAULT_LOCAL_REGULAR_THREADS_PER_DRAIN_TASK = 16;

public:
	DUCKDB_API AsyncFileWriter(QueryContext context, FileSystem &fs, const string &path,
	                           FileOpenFlags open_flags = DEFAULT_OPEN_FLAGS);
	DUCKDB_API ~AsyncFileWriter() override;

public:
	//! Copy the provided bytes into owned storage and register them for asynchronous writing.
	DUCKDB_API void WriteData(const_data_ptr_t buffer, idx_t write_size) override;
	//! Transfer ownership of an existing write buffer and register it without copying.
	DUCKDB_API void WriteData(unique_ptr<AsyncWriteBuffer> buffer);

	//! Delay async task scheduling while the returned guard is alive. Flush/Close still drain registered writes.
	DUCKDB_API BatchGuard StartBatch();
	//! Flush this WriteStream by waiting until all registered writes have reached the file handle.
	DUCKDB_API void Flush();
	//! Wait until all registered writes have reached the file handle, and rethrow any async write error.
	DUCKDB_API void WaitAll();
	//! Help drain async writes when pending bytes exceed the current memory budget. No-op while a batch is open.
	DUCKDB_API void ApplyBackpressure();
	//! Wait for all writes, then close the file handle.
	DUCKDB_API void Close();
	//! Wait for all writes, then fsync the file handle.
	DUCKDB_API void Sync();
	//! Wait for all writes, then truncate the file to the requested logical size.
	DUCKDB_API void Truncate(idx_t size);

	//! Return the logical file size, including writes that have been registered but not drained yet.
	DUCKDB_API idx_t GetFileSize();
	//! Return the logical number of bytes written, including writes that are still pending.
	DUCKDB_API idx_t GetTotalWritten() const;

private:
	//! Whether registering a buffer may schedule an async drain task immediately.
	enum class ScheduleMode : uint8_t { ALLOW, DEFER };
	//! Whether to force a TemporaryMemoryState growth check while a registration batch is open.
	enum class MemoryUpdateMode : uint8_t { COARSE, FORCE };
	//! Whether to schedule only enough task capacity for normal overlap, or force all pending bytes to drain.
	enum class SchedulePolicy : uint8_t { THRESHOLD, FORCE };
	//! Whether async drain tasks can write independent file ranges concurrently.
	enum class DrainMode : uint8_t { SEQUENTIAL, POSITIONAL };
	//! Whether waiting for scheduled writes should preserve an open registration batch.
	enum class BatchDrainMode : uint8_t { PRESERVE_BATCH, FORCE_CLOSE_BATCH };

	//! Owned write data with the logical file offset assigned at registration.
	struct PendingWrite {
		PendingWrite(unique_ptr<AsyncWriteBuffer> buffer, idx_t offset);

		idx_t Size() const;

		unique_ptr<AsyncWriteBuffer> buffer;
		idx_t offset;
	};

	//! Register an owned buffer for writing, using the configured synchronous/asynchronous mode.
	void RegisterWrite(unique_ptr<AsyncWriteBuffer> buffer, ScheduleMode schedule_mode = ScheduleMode::ALLOW);
	//! Register an owned buffer whose bytes were already counted in total_written.
	void RegisterStagedWrite(unique_ptr<AsyncWriteBuffer> buffer, idx_t offset,
	                         ScheduleMode schedule_mode = ScheduleMode::ALLOW);
	//! Add a buffer with its assigned file offset to the configured sync/async write path.
	void RegisterWriteInternal(unique_ptr<AsyncWriteBuffer> buffer, idx_t offset, ScheduleMode schedule_mode);
	//! Write caller-owned bytes through the local staging buffer when async draining is disabled.
	void WriteDataSynchronously(data_ptr_t buffer, idx_t write_size);
	//! Move any staged copied bytes into the pending write queue.
	void SealCopiedBuffer(ScheduleMode schedule_mode = ScheduleMode::ALLOW);

	//! Seal copied bytes, then schedule as many drain tasks as the pending queue allows.
	void SchedulePendingWrites(SchedulePolicy policy = SchedulePolicy::THRESHOLD);
	//! Schedule drain tasks from already registered pending writes. Safe to call from async drain tasks.
	void SchedulePendingWritesInternal(SchedulePolicy policy = SchedulePolicy::THRESHOLD);
	//! Enter a registration batch, delaying async draining until EndBatch.
	void BeginBatch();
	//! Leave a registration batch and schedule pending writes when the outermost batch closes.
	void EndBatch();

	//! Grow the TemporaryMemoryState reservation coarsely; it is released only when the writer closes.
	void UpdateMemoryState(MemoryUpdateMode mode = MemoryUpdateMode::COARSE);
	//! Return the current async backlog budget after applying the fixed writer cap.
	idx_t BackpressureBudget();
	//! Effective byte budget for one drain task, never smaller than the coalescing threshold.
	idx_t DrainTaskByteBudget() const;
	//! Return queued/in-flight bytes that have not reached the file handle yet. Caller must hold lock.
	idx_t TotalPendingBytes() const;
	//! Return pending bytes that are not already covered by scheduled-but-not-started drain task capacity.
	idx_t UnscheduledPendingBytes() const;
	//! Minimum pending bytes before threshold scheduling starts the first task.
	idx_t FirstTaskScheduleThreshold() const;
	//! Probe whether the file system supports independent positional writes.
	bool SupportsPositionalWrites();
	//! Estimate how many drain tasks are useful for the currently queued writes. Caller must hold lock.
	idx_t EstimateScheduleCount(idx_t available_slots, SchedulePolicy policy) const;
	//! Mark that a scheduled drain task has started and will claim its byte budget.
	void StartDrainTask();
	//! Move one byte-budgeted prefix of pending writes into a drain task.
	idx_t TakePendingWrites(vector<PendingWrite> &writes);
	//! Release one scheduled/running drain task slot and its in-flight byte accounting.
	void FinishDrainTask(idx_t in_flight_task_bytes);
	//! Release a task slot for a scheduled task that never entered the writer because another task failed.
	void CancelScheduledDrainTask();
	//! Release multiple reserved task slots that were never scheduled.
	void CancelScheduledDrainTasks(idx_t task_count);

	//! Async task entry point that drains one budgeted batch of pending buffers.
	void DrainPendingWrites();
	//! Write pending buffers in registration order, coalescing adjacent small writes.
	idx_t WritePendingWrites(vector<PendingWrite> &writes);
	//! Write bytes to the underlying file handle at the assigned logical stream offset.
	void WriteBuffer(data_ptr_t buffer, idx_t size, idx_t offset);
	//! Surface an error thrown by an async drain task.
	void RethrowTaskError();
	//! Wait for scheduled writes, optionally restoring an active registration batch afterwards.
	void WaitAllInternal(BatchDrainMode batch_drain_mode);
	//! Release the writer's TemporaryMemoryState reservation.
	void ReleaseMemoryReservation();
	//! Resolve constants that depend on the file system and scheduler state.
	void ResolveWriteSettings();

private:
	QueryContext context;
	ClientContext &client_context;
	FileSystem &fs;
	string path;
	unique_ptr<FileHandle> handle;

	//! Temporary memory reservation state used to limit queued async write data.
	unique_ptr<TemporaryMemoryState> memory_state;

	//! Copy staging buffer for small transient WriteData inputs. Only accessed by the registering thread.
	unique_ptr<CopiedAsyncWriteBuffer> copied_buffer;
	//! Logical file offset of the first byte in copied_buffer.
	idx_t copied_buffer_offset = 0;
	//! Logical stream position, including copied/staged/pending bytes. Updated by the registering thread.
	idx_t total_written = 0;
	//! Set once the handle has been closed or detached.
	bool closed = false;

	//! Drain-time coalescing threshold, resolved once from the file system type.
	idx_t coalesce_threshold = 0;
	//! Minimum pending bytes requested from TemporaryMemoryManager while writes are outstanding.
	idx_t min_pending_bytes = 0;
	//! Hard cap over the TemporaryMemoryState reservation.
	idx_t max_pending_bytes = 0;
	//! Last remaining-size request sent to TemporaryMemoryManager. Grows monotonically until close.
	idx_t memory_request_bytes = 0;
	//! Whether this writer targets a local file handle.
	bool local_file = false;
	//! Whether async tasks may drain independent ranges concurrently using positional writes.
	DrainMode drain_mode = DrainMode::SEQUENTIAL;
	//! Maximum number of scheduled/running drain tasks for this writer.
	idx_t max_active_drain_tasks = 1;

	//! Protects state shared between the registering thread and async drain tasks.
	mutex lock;
	//! Pending buffers in registration order with pre-assigned logical file offsets.
	vector<PendingWrite> pending_writes;
	//! Bytes queued in pending_writes that have not been taken by a drain task yet.
	idx_t pending_bytes = 0;
	//! Bytes owned by drain tasks that have not reached the file handle yet.
	idx_t in_flight_bytes = 0;
	//! Nested batch depth. While non-zero, async draining and backpressure are delayed.
	idx_t batch_depth = 0;
	//! Scheduled or running drain tasks for this writer.
	idx_t active_drain_tasks = 0;
	//! Scheduled drain tasks that have not yet claimed pending bytes.
	idx_t pending_drain_tasks = 0;

	//! Async task executor. If absent, writes are performed synchronously on registration.
	//! Keep this after task-accounting fields so queued task destructors can still release slots.
	unique_ptr<TaskExecutor> executor;
};

} // namespace duckdb
