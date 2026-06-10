#include "duckdb/common/serializer/async_file_writer.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cstring>

namespace duckdb {

namespace {

class CopiedAsyncWriteBuffer : public AsyncWriteBuffer {
public:
	explicit CopiedAsyncWriteBuffer(idx_t size_p)
	    : data(make_unsafe_uniq_array_uninitialized<data_t>(size_p)), size(size_p) {
	}

	const_data_ptr_t Ptr() const override {
		return data.get();
	}

	idx_t Size() const override {
		return size;
	}

	data_ptr_t MutablePtr() {
		return data.get();
	}

private:
	unsafe_unique_array<data_t> data;
	idx_t size;
};

static idx_t ClampWriterWatermark(idx_t value) {
	constexpr idx_t MINIMUM_WATERMARK = 64ULL * 1024ULL * 1024ULL;
	constexpr idx_t MAXIMUM_WATERMARK = 512ULL * 1024ULL * 1024ULL;
	return MinValue<idx_t>(MaxValue<idx_t>(value, MINIMUM_WATERMARK), MAXIMUM_WATERMARK);
}

} // namespace

class AsyncFileWriterTask : public BaseExecutorTask {
public:
	AsyncFileWriterTask(AsyncFileWriter &writer_p, TaskExecutor &executor)
	    : BaseExecutorTask(executor), writer(writer_p) {
	}

	void ExecuteTask() override {
		writer.DrainPendingWrites();
	}

private:
	AsyncFileWriter &writer;
};

AsyncFileWriter::AsyncFileWriter(QueryContext context_p, FileSystem &fs_p, const string &path_p,
                                 FileOpenFlags open_flags, AsyncFileWriterOptions options)
    : context(context_p), fs(fs_p), path(path_p) {
	handle = fs.OpenFile(path, open_flags | FileLockType::WRITE_LOCK);
	ResolveOptions(options);
	if (context.GetClientContext()) {
		executor = make_uniq<TaskExecutor>(*context.GetClientContext(), TaskSchedulerType::ASYNC);
	}
}

AsyncFileWriter::~AsyncFileWriter() {
	if (!closed && handle) {
		try {
			Close();
		} catch (...) {
		}
	}
}

void AsyncFileWriter::ResolveOptions(AsyncFileWriterOptions options) {
	bool local_file = fs.IsLocalFileSystem();
	if (!local_file && handle) {
		try {
			local_file = handle->OnDiskFile();
		} catch (...) {
			local_file = false;
		}
	}

	coalesce_threshold = local_file ? options.local_coalesce_threshold : options.remote_coalesce_threshold;
	if (coalesce_threshold == 0) {
		coalesce_threshold = FILE_BUFFER_SIZE;
	}

	idx_t resolved_high_watermark;
	if (options.high_watermark != 0) {
		resolved_high_watermark = options.high_watermark;
	} else if (context.GetClientContext()) {
		auto &buffer_manager = BufferManager::GetBufferManager(*context.GetClientContext());
		resolved_high_watermark = ClampWriterWatermark(buffer_manager.GetOperatorMemoryLimit() / 16);
	} else {
		resolved_high_watermark = 512ULL * 1024ULL * 1024ULL;
	}
	high_watermark = MaxValue<idx_t>(resolved_high_watermark, coalesce_threshold * 2);
	low_watermark = options.low_watermark != 0 ? options.low_watermark : high_watermark / 2;
	low_watermark = MinValue<idx_t>(low_watermark, high_watermark);
}

FileHandle &AsyncFileWriter::GetFileHandle() {
	D_ASSERT(handle);
	return *handle;
}

idx_t AsyncFileWriter::GetFileSize() {
	return GetTotalWritten();
}

idx_t AsyncFileWriter::GetTotalWritten() const {
	lock_guard<mutex> guard(lock);
	return total_written;
}

void AsyncFileWriter::WriteData(const_data_ptr_t buffer, idx_t write_size) {
	if (write_size == 0) {
		return;
	}
	auto owned_buffer = make_uniq<CopiedAsyncWriteBuffer>(write_size);
	memcpy(owned_buffer->MutablePtr(), buffer, write_size);
	RegisterWrite(std::move(owned_buffer));
}

void AsyncFileWriter::WriteData(unique_ptr<AsyncWriteBuffer> buffer) {
	if (!buffer || buffer->Size() == 0) {
		return;
	}
	RegisterWrite(std::move(buffer));
}

void AsyncFileWriter::RegisterWrite(unique_ptr<AsyncWriteBuffer> buffer) {
	RethrowTaskError();
	if (closed) {
		throw IOException("Cannot write to closed file \"%s\"", path);
	}

	if (!executor) {
		WriteBuffer(buffer->Ptr(), buffer->Size());
		lock_guard<mutex> guard(lock);
		total_written += buffer->Size();
		return;
	}

	bool should_schedule = false;
	{
		lock_guard<mutex> guard(lock);
		pending_writes.push_back({total_written, std::move(buffer)});
		pending_bytes += pending_writes.back().buffer->Size();
		total_written += pending_writes.back().buffer->Size();
		if (!task_scheduled && batch_depth == 0) {
			task_scheduled = true;
			should_schedule = true;
		}
	}
	if (should_schedule) {
		ScheduleTask();
	}
}

void AsyncFileWriter::ScheduleTask() {
	D_ASSERT(executor);
	executor->ScheduleTask(make_uniq<AsyncFileWriterTask>(*this, *executor));
}

void AsyncFileWriter::DrainPendingWrites() {
	while (true) {
		vector<PendingWrite> writes;
		{
			lock_guard<mutex> guard(lock);
			if (pending_writes.empty()) {
				task_scheduled = false;
				return;
			}
			writes = std::move(pending_writes);
			pending_writes.clear();
		}
		WritePendingWrites(writes);
		{
			lock_guard<mutex> guard(lock);
			for (auto &write : writes) {
				D_ASSERT(pending_bytes >= write.buffer->Size());
				pending_bytes -= write.buffer->Size();
			}
		}
	}
}

void AsyncFileWriter::BeginBatch() {
	if (!executor) {
		return;
	}
	lock_guard<mutex> guard(lock);
	batch_depth++;
}

void AsyncFileWriter::EndBatch() {
	if (!executor) {
		return;
	}
	bool should_schedule = false;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(batch_depth > 0);
		batch_depth--;
		if (batch_depth == 0 && !task_scheduled && !pending_writes.empty()) {
			task_scheduled = true;
			should_schedule = true;
		}
	}
	if (should_schedule) {
		ScheduleTask();
	}
}

void AsyncFileWriter::WritePendingWrites(vector<PendingWrite> &writes) {
	idx_t i = 0;
	while (i < writes.size()) {
		auto &write = writes[i];
		auto write_size = write.buffer->Size();
		if (write_size >= coalesce_threshold) {
			WriteBuffer(write.buffer->Ptr(), write_size);
			i++;
			continue;
		}

		idx_t coalesced_size = 0;
		idx_t end = i;
		while (end < writes.size()) {
			auto next_size = writes[end].buffer->Size();
			if (next_size >= coalesce_threshold || coalesced_size + next_size > coalesce_threshold) {
				break;
			}
			coalesced_size += next_size;
			end++;
		}
		if (end == i + 1) {
			WriteBuffer(write.buffer->Ptr(), write_size);
			i++;
			continue;
		}

		auto coalesced = make_unsafe_uniq_array_uninitialized<data_t>(coalesced_size);
		idx_t offset = 0;
		for (idx_t write_idx = i; write_idx < end; write_idx++) {
			auto &current = writes[write_idx];
			memcpy(coalesced.get() + offset, current.buffer->Ptr(), current.buffer->Size());
			offset += current.buffer->Size();
		}
		WriteBuffer(coalesced.get(), coalesced_size);
		i = end;
	}
}

void AsyncFileWriter::WriteBuffer(const_data_ptr_t buffer, idx_t size) {
	handle->Write(context, const_cast<data_ptr_t>(buffer), size);
}

void AsyncFileWriter::RethrowTaskError() {
	if (executor && executor->HasError()) {
		executor->ThrowError();
	}
}

void AsyncFileWriter::Flush() {
	RethrowTaskError();
}

void AsyncFileWriter::ApplyBackpressure() {
	if (!executor) {
		return;
	}
	RethrowTaskError();
	while (true) {
		{
			lock_guard<mutex> guard(lock);
			if (pending_bytes <= high_watermark) {
				return;
			}
		}
		executor->WorkOnTasks();
		RethrowTaskError();
		{
			lock_guard<mutex> guard(lock);
			if (pending_bytes <= low_watermark) {
				return;
			}
		}
	}
}

void AsyncFileWriter::WaitAll() {
	if (executor) {
		executor->WorkOnTasks();
	}
	RethrowTaskError();
}

void AsyncFileWriter::Close() {
	if (closed) {
		return;
	}
	WaitAll();
	handle->Close();
	handle.reset();
	closed = true;
}

void AsyncFileWriter::Sync() {
	WaitAll();
	handle->Sync();
}

void AsyncFileWriter::Truncate(idx_t size) {
	WaitAll();
	handle->Truncate(NumericCast<int64_t>(size));
	{
		lock_guard<mutex> guard(lock);
		total_written = size;
	}
	if (handle->CanSeek() && handle->SeekPosition() > size) {
		handle->Seek(size);
	}
}

} // namespace duckdb
