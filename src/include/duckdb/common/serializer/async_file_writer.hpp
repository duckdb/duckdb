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
//! This is a sequential stream writer: logical offsets are assigned when writes are registered via GetTotalWritten(),
//! but pending writes are always drained to the file handle in registration order.
//! Calls into this writer must be externally serialized; internal locking only coordinates with async drain tasks.
class AsyncFileWriter : public WriteStream {
	friend class AsyncFileWriterTask;

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

	static constexpr FileOpenFlags DEFAULT_OPEN_FLAGS = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;
	//! Capacity of the staging buffer used for small transient WriteData inputs.
	static constexpr idx_t DEFAULT_COPIED_BUFFER_CAPACITY = 4096;
	//! Local file systems are cheap to call, so only coalesce up to the buffered writer page size.
	static constexpr idx_t DEFAULT_LOCAL_COALESCE_THRESHOLD = 4096;
	//! Remote file systems benefit from fewer round trips, so coalesce contiguous small buffers more aggressively.
	static constexpr idx_t DEFAULT_REMOTE_COALESCE_THRESHOLD = 8ULL * 1024ULL * 1024ULL;
	//! Maximum queued async bytes retained per regular execution thread.
	static constexpr idx_t DEFAULT_MAX_PENDING_BYTES_PER_THREAD = 128ULL * 1024ULL * 1024ULL;

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
	//! Whether registering a buffer should advance the logical stream position.
	enum class WriteAccounting : uint8_t { ADD_TO_TOTAL_WRITTEN, ALREADY_COUNTED };
	//! Whether registering a buffer may schedule an async drain task immediately.
	enum class ScheduleMode : uint8_t { ALLOW, DEFER };
	//! Whether to force a TemporaryMemoryState update or apply coarse update filtering.
	enum class MemoryUpdateMode : uint8_t { COARSE, FORCE };

	//! Register an owned buffer for writing, using the configured synchronous/asynchronous mode.
	void RegisterWrite(unique_ptr<AsyncWriteBuffer> buffer,
	                   WriteAccounting accounting = WriteAccounting::ADD_TO_TOTAL_WRITTEN,
	                   ScheduleMode schedule_mode = ScheduleMode::ALLOW);
	//! Move any staged copied bytes into the pending write queue.
	void SealCopiedBuffer(ScheduleMode schedule_mode = ScheduleMode::ALLOW);
	//! Schedule an async drain task. The caller must have marked task_scheduled.
	void ScheduleTask();
	//! Schedule draining when pending writes exist and no drain task is already scheduled.
	void SchedulePendingWrites();
	//! Enter a registration batch, delaying async draining until EndBatch.
	void BeginBatch();
	//! Leave a registration batch and schedule pending writes when the outermost batch closes.
	void EndBatch();
	//! Report queued bytes to TemporaryMemoryState, avoiding per-write updates in the common path.
	void UpdateMemoryState(MemoryUpdateMode mode = MemoryUpdateMode::COARSE);
	//! Return the current async backlog budget after applying the fixed writer cap.
	idx_t BackpressureBudget();
	//! Async task entry point that drains pending buffers to the file handle.
	void DrainPendingWrites();
	//! Write pending buffers in registration order, coalescing adjacent small writes.
	idx_t WritePendingWrites(vector<unique_ptr<AsyncWriteBuffer>> &writes);
	//! Write bytes to the underlying file handle at its current stream position.
	void WriteBuffer(data_ptr_t buffer, idx_t size);
	//! Surface an error thrown by an async drain task.
	void RethrowTaskError();
	//! Resolve constants that depend on the file system and scheduler state.
	void ResolveWriteSettings();

private:
	QueryContext context;
	ClientContext &client_context;
	FileSystem &fs;
	string path;
	unique_ptr<FileHandle> handle;

	//! Async task executor. If absent, writes are performed synchronously on registration.
	unique_ptr<TaskExecutor> executor;
	//! Temporary memory reservation state used to limit queued async write data.
	unique_ptr<TemporaryMemoryState> memory_state;

	//! Copy staging buffer for small transient WriteData inputs. Only accessed by the registering thread.
	unique_ptr<CopiedAsyncWriteBuffer> copied_buffer;
	//! Logical stream position, including copied/staged/pending bytes. Updated by the registering thread.
	idx_t total_written = 0;
	//! Set once the handle has been closed or detached.
	bool closed = false;

	//! Drain-time coalescing threshold, resolved once from the file system type.
	idx_t coalesce_threshold = 0;
	//! Hard cap over the TemporaryMemoryState reservation.
	idx_t max_pending_bytes = 0;

	//! Protects state shared between the registering thread and async drain tasks.
	mutex lock;
	//! Pending buffers in registration order. No per-write offset is needed for this sequential stream writer.
	vector<unique_ptr<AsyncWriteBuffer>> pending_writes;
	//! Bytes in pending_writes that have not reached the file handle yet.
	idx_t pending_bytes = 0;
	//! Last pending byte count reported to TemporaryMemoryState.
	idx_t memory_state_pending_bytes = 0;
	//! Nested batch depth. While non-zero, async draining and backpressure are delayed.
	idx_t batch_depth = 0;
	//! Whether a drain task has already been scheduled for the current pending queue.
	bool task_scheduled = false;
};

} // namespace duckdb
