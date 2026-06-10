//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/async_file_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/main/query_context.hpp"

namespace duckdb {

class AsyncWriteBuffer {
public:
	virtual ~AsyncWriteBuffer() = default;

	virtual const_data_ptr_t Ptr() const = 0;
	virtual idx_t Size() const = 0;
};

struct AsyncFileWriterOptions {
	idx_t local_coalesce_threshold = 1024ULL * 1024ULL;
	idx_t remote_coalesce_threshold = 8ULL * 1024ULL * 1024ULL;
	idx_t high_watermark = 0;
	idx_t low_watermark = 0;
};

class AsyncFileWriter : public WriteStream {
	friend class AsyncFileWriterTask;

public:
	static constexpr FileOpenFlags DEFAULT_OPEN_FLAGS = BufferedFileWriter::DEFAULT_OPEN_FLAGS;

	DUCKDB_API AsyncFileWriter(QueryContext context, FileSystem &fs, const string &path,
	                           FileOpenFlags open_flags = DEFAULT_OPEN_FLAGS,
	                           AsyncFileWriterOptions options = AsyncFileWriterOptions());
	DUCKDB_API ~AsyncFileWriter() override;

	DUCKDB_API void WriteData(const_data_ptr_t buffer, idx_t write_size) override;
	DUCKDB_API void WriteData(unique_ptr<AsyncWriteBuffer> buffer);

	DUCKDB_API void BeginBatch();
	DUCKDB_API void EndBatch();
	DUCKDB_API void Flush();
	DUCKDB_API void WaitAll();
	DUCKDB_API void ApplyBackpressure();
	DUCKDB_API void Close();
	DUCKDB_API void Sync();
	DUCKDB_API void Truncate(idx_t size);

	DUCKDB_API idx_t GetFileSize();
	DUCKDB_API idx_t GetTotalWritten() const;
	DUCKDB_API FileHandle &GetFileHandle();

private:
	struct PendingWrite {
		idx_t offset;
		unique_ptr<AsyncWriteBuffer> buffer;
	};

private:
	void RegisterWrite(unique_ptr<AsyncWriteBuffer> buffer);
	void ScheduleTask();
	void DrainPendingWrites();
	void WritePendingWrites(vector<PendingWrite> &writes);
	void WriteBuffer(const_data_ptr_t buffer, idx_t size);
	void RethrowTaskError();
	void ResolveOptions(AsyncFileWriterOptions options);

private:
	QueryContext context;
	FileSystem &fs;
	string path;
	unique_ptr<FileHandle> handle;

	unique_ptr<class TaskExecutor> executor;

	mutable mutex lock;
	vector<PendingWrite> pending_writes;
	idx_t pending_bytes = 0;
	idx_t total_written = 0;
	idx_t batch_depth = 0;
	bool task_scheduled = false;
	bool closed = false;

	idx_t coalesce_threshold = 0;
	idx_t high_watermark = 0;
	idx_t low_watermark = 0;
};

} // namespace duckdb
