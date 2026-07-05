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
#include "duckdb/common/serializer/async_write_queue.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/main/query_context.hpp"

namespace duckdb {

class ClientContext;
class CopiedAsyncWriteBuffer;

//! WriteStream implementation that registers writes cheaply and drains them on the async task scheduler.
//! This is a logical stream writer: offsets are assigned when writes are registered via GetTotalWritten().
//! Physical writes may complete out of order when positional writes are supported; WaitAll/Close complete the file.
//! Calls into this writer must be externally serialized; internal locking only coordinates with async drain tasks.
class AsyncFileWriter : public WriteStream, private ManagedAsyncWriteStreamTarget {
public:
	//! RAII handle that batches write registration. Finish() must be called on the normal path to leave the batch and
	//! start draining; scope exit only leaves the batch as exception cleanup.
	class BatchGuard {
	public:
		BatchGuard(const BatchGuard &) = delete;
		BatchGuard &operator=(const BatchGuard &) = delete;
		DUCKDB_API BatchGuard(BatchGuard &&other) noexcept;
		BatchGuard &operator=(BatchGuard &&other) = delete;
		DUCKDB_API ~BatchGuard();

	public:
		//! Leave the batch and apply the writer's normal post-batch scheduling/backpressure policy.
		DUCKDB_API void Finish();

	private:
		friend class AsyncFileWriter;

		DUCKDB_API explicit BatchGuard(AsyncFileWriter &writer);

	private:
		optional_ptr<AsyncFileWriter> writer;
	};

	//! Default file-open behavior for creating a write-locked output file.
	static constexpr FileOpenFlags DEFAULT_OPEN_FLAGS = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;

public:
	DUCKDB_API AsyncFileWriter(QueryContext context, FileSystem &fs, const string &path,
	                           FileOpenFlags open_flags = DEFAULT_OPEN_FLAGS);
	DUCKDB_API ~AsyncFileWriter() override;
	using WriteStream::Write;

public:
	//! Copy the provided bytes into owned storage and register them for asynchronous writing.
	DUCKDB_API void WriteData(const_data_ptr_t buffer, idx_t write_size) override;
	//! Transfer ownership of an existing write buffer and register it without copying.
	DUCKDB_API void WriteData(unique_ptr<AsyncWriteBuffer> buffer);

	//! Delay async task scheduling while the returned guard is alive.
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
	using BatchDrainMode = ManagedAsyncWriteStreamQueue::BatchDrainMode;
	using ScheduleMode = ManagedAsyncWriteStreamQueue::ScheduleMode;
	using SchedulePolicy = ManagedAsyncWriteStreamQueue::SchedulePolicy;

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
	//! Enter a registration batch, delaying async draining until the batch is left.
	void BeginBatch();
	//! Leave a registration batch without scheduling, blocking, or throwing.
	void LeaveBatch() noexcept;

	//! Return whether the file handle supports independent positional writes.
	bool SupportsPositionalWrites() override;
	//! Return whether this writer targets a local file-like handle.
	bool IsLocalFile() override;
	//! Write bytes to the underlying file handle at the assigned logical stream offset.
	void Write(data_ptr_t buffer, idx_t size, idx_t offset) override;
	//! Write bytes to the underlying file handle's current position.
	void Write(data_ptr_t buffer, idx_t size) override;
	//! Surface an error thrown by an async drain task.
	void RethrowTaskError();
	//! Wait for scheduled writes, optionally restoring an active registration batch afterwards.
	void WaitAllInternal(BatchDrainMode batch_drain_mode);

private:
	QueryContext context;
	ClientContext &client_context;
	FileSystem &fs;
	string path;
	unique_ptr<FileHandle> handle;
	//! Managed queue that owns stream scheduling, backpressure, and write coalescing.
	unique_ptr<ManagedAsyncWriteStreamQueue> write_queue;

	//! Copy staging buffer for small transient WriteData inputs. Only accessed by the registering thread.
	unique_ptr<CopiedAsyncWriteBuffer> copied_buffer;
	//! Logical file offset of the first byte in copied_buffer.
	idx_t copied_buffer_offset = 0;
	//! Logical stream position, including copied/staged/pending bytes. Updated by the registering thread.
	idx_t total_written = 0;
	//! Set once the handle has been closed or detached.
	bool closed = false;
};

} // namespace duckdb
