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
class TemporaryMemoryState;

class AsyncWriteBuffer {
public:
	virtual ~AsyncWriteBuffer() = default;

	//! Pointer to the bytes to write. The buffer must remain valid for the lifetime of this object.
	virtual data_ptr_t Ptr() = 0;
	//! Number of bytes exposed by Ptr().
	virtual idx_t Size() const = 0;
};

struct AsyncFileWriterOptions {
	static constexpr idx_t DEFAULT_LOCAL_COALESCE_THRESHOLD = 4096;
	static constexpr idx_t DEFAULT_REMOTE_COALESCE_THRESHOLD = 8ULL * 1024ULL * 1024ULL;
	static constexpr idx_t DEFAULT_MAX_PENDING_BYTES_PER_THREAD = 128ULL * 1024ULL * 1024ULL;

	//! Maximum byte size for coalescing small writes on local file systems.
	idx_t local_coalesce_threshold = DEFAULT_LOCAL_COALESCE_THRESHOLD;
	//! Maximum byte size for coalescing small writes on remote file systems.
	idx_t remote_coalesce_threshold = DEFAULT_REMOTE_COALESCE_THRESHOLD;
	//! Maximum async backlog retained per regular execution thread.
	idx_t max_pending_bytes_per_thread = DEFAULT_MAX_PENDING_BYTES_PER_THREAD;
};

//! WriteStream implementation that registers writes cheaply and drains them on the async task scheduler.
//! This is a sequential stream writer: logical offsets are assigned when writes are registered via GetTotalWritten(),
//! but pending writes are always drained to the file handle in registration order.
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

public:
	DUCKDB_API AsyncFileWriter(QueryContext context, FileSystem &fs, const string &path,
	                           FileOpenFlags open_flags = DEFAULT_OPEN_FLAGS,
	                           AsyncFileWriterOptions options = AsyncFileWriterOptions());
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
	void RegisterWrite(unique_ptr<AsyncWriteBuffer> buffer);
	void ScheduleTask();
	void SchedulePendingWrites();
	void BeginBatch();
	void EndBatch();
	void UpdateMemoryState(bool force = false);
	idx_t BackpressureBudget();
	void DrainPendingWrites();
	void WritePendingWrites(vector<unique_ptr<AsyncWriteBuffer>> &writes);
	void WriteBuffer(data_ptr_t buffer, idx_t size);
	void RethrowTaskError();
	void ResolveOptions(AsyncFileWriterOptions options);

private:
	QueryContext context;
	ClientContext &client_context;
	FileSystem &fs;
	string path;
	unique_ptr<FileHandle> handle;

	unique_ptr<class TaskExecutor> executor;
	unique_ptr<TemporaryMemoryState> memory_state;

	mutable mutex lock;
	//! Pending buffers are kept in registration order. No per-write offset is needed for this stream writer.
	vector<unique_ptr<AsyncWriteBuffer>> pending_writes;
	idx_t pending_bytes = 0;
	idx_t memory_state_pending_bytes = 0;
	idx_t total_written = 0;
	idx_t batch_depth = 0;
	bool task_scheduled = false;
	bool closed = false;

	idx_t coalesce_threshold = 0;
	idx_t max_pending_bytes = 0;
};

} // namespace duckdb
