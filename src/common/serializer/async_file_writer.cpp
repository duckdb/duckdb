#include "duckdb/common/serializer/async_file_writer.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

#include <cstring>

namespace duckdb {

namespace {

class CopiedAsyncWriteBuffer : public AsyncWriteBuffer {
public:
	CopiedAsyncWriteBuffer(ClientContext &context, idx_t size_p)
	    : data(BufferAllocator::Get(context).Allocate(size_p)), size(size_p) {
	}

	data_ptr_t Ptr() override {
		return data.get();
	}

	idx_t Size() const override {
		return size;
	}

private:
	AllocatedData data;
	idx_t size;
};

static ClientContext &RequireClientContext(QueryContext context) {
	auto client_context = context.GetClientContext();
	if (!client_context) {
		throw InvalidInputException("AsyncFileWriter requires a ClientContext");
	}
	return *client_context;
}

static idx_t MultiplyCap(idx_t lhs, idx_t rhs) {
	if (lhs != 0 && rhs > NumericLimits<idx_t>::Maximum() / lhs) {
		return NumericLimits<idx_t>::Maximum();
	}
	return lhs * rhs;
}

static idx_t MemoryStateUpdateGranularity(idx_t max_pending_bytes, idx_t coalesce_threshold) {
	constexpr idx_t MINIMUM_UPDATE_GRANULARITY = 1024ULL * 1024ULL;
	auto threshold = max_pending_bytes / 16;
	threshold = MaxValue(threshold, coalesce_threshold);
	return MaxValue(threshold, MINIMUM_UPDATE_GRANULARITY);
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
    : context(context_p), client_context(RequireClientContext(context_p)), fs(fs_p), path(path_p) {
	handle = fs.OpenFile(path, open_flags | FileLockType::WRITE_LOCK);
	ResolveOptions(options);
	auto &scheduler = TaskScheduler::GetScheduler(client_context);
	if (scheduler.NumberOfAsyncThreads() > 0) {
		executor = make_uniq<TaskExecutor>(client_context, TaskSchedulerType::ASYNC);
		memory_state = TemporaryMemoryManager::Get(client_context).Register(client_context);
		memory_state->SetZero();
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

AsyncFileWriter::BatchGuard::BatchGuard(AsyncFileWriter &writer_p) : writer(writer_p) {
	writer->BeginBatch();
}

AsyncFileWriter::BatchGuard::BatchGuard(BatchGuard &&other) noexcept : writer(other.writer) {
	other.writer = nullptr;
}

AsyncFileWriter::BatchGuard::~BatchGuard() {
	if (writer) {
		writer->EndBatch();
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
		coalesce_threshold = AsyncFileWriterOptions::DEFAULT_LOCAL_COALESCE_THRESHOLD;
	}

	auto &scheduler = TaskScheduler::GetScheduler(client_context);
	auto regular_threads = MaxValue<idx_t>(NumericCast<idx_t>(scheduler.NumberOfThreads()), 1);
	max_pending_bytes = MultiplyCap(options.max_pending_bytes_per_thread, regular_threads);
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
	auto owned_buffer = make_uniq<CopiedAsyncWriteBuffer>(client_context, write_size);
	memcpy(owned_buffer->Ptr(), buffer, write_size);
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
		pending_bytes += buffer->Size();
		total_written += buffer->Size();
		pending_writes.push_back(std::move(buffer));
		if (!task_scheduled && batch_depth == 0) {
			task_scheduled = true;
			should_schedule = true;
		}
	}
	if (should_schedule) {
		UpdateMemoryState();
		ScheduleTask();
	} else {
		UpdateMemoryState();
	}
}

void AsyncFileWriter::ScheduleTask() {
	D_ASSERT(executor);
	executor->ScheduleTask(make_uniq<AsyncFileWriterTask>(*this, *executor));
}

AsyncFileWriter::BatchGuard AsyncFileWriter::StartBatch() {
	return BatchGuard(*this);
}

void AsyncFileWriter::SchedulePendingWrites() {
	if (!executor) {
		return;
	}
	bool should_schedule = false;
	{
		lock_guard<mutex> guard(lock);
		if (!task_scheduled && !pending_writes.empty()) {
			task_scheduled = true;
			should_schedule = true;
		}
	}
	if (should_schedule) {
		ScheduleTask();
	}
}

void AsyncFileWriter::UpdateMemoryState(bool force) {
	if (!memory_state) {
		return;
	}

	// TMM is shared state; report coarse backlog changes instead of touching it for every registered write.
	idx_t current_pending_bytes;
	bool should_update;
	{
		lock_guard<mutex> guard(lock);
		if (batch_depth > 0 && !force) {
			return;
		}
		current_pending_bytes = pending_bytes;
		auto update_granularity = MemoryStateUpdateGranularity(max_pending_bytes, coalesce_threshold);
		auto grew_enough = current_pending_bytes > memory_state_pending_bytes &&
		                   current_pending_bytes - memory_state_pending_bytes >= update_granularity;
		should_update = force || current_pending_bytes == 0 || memory_state_pending_bytes == 0 || grew_enough ||
		                current_pending_bytes <= memory_state_pending_bytes / 2;
		if (!should_update) {
			return;
		}
		memory_state_pending_bytes = current_pending_bytes;
	}
	if (current_pending_bytes == 0) {
		memory_state->SetZero();
	} else {
		memory_state->SetRemainingSizeAndUpdateReservation(client_context, current_pending_bytes);
	}
}

idx_t AsyncFileWriter::BackpressureBudget() {
	if (!memory_state) {
		return NumericLimits<idx_t>::Maximum();
	}
	return MinValue(memory_state->GetReservation(), max_pending_bytes);
}

void AsyncFileWriter::DrainPendingWrites() {
	while (true) {
		vector<unique_ptr<AsyncWriteBuffer>> writes;
		{
			lock_guard<mutex> guard(lock);
			if (pending_writes.empty() || batch_depth > 0) {
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
				D_ASSERT(pending_bytes >= write->Size());
				pending_bytes -= write->Size();
			}
		}
		UpdateMemoryState();
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
	bool batch_done = false;
	{
		lock_guard<mutex> guard(lock);
		if (batch_depth == 0) {
			return;
		}
		batch_depth--;
		batch_done = batch_depth == 0;
	}
	if (batch_done) {
		UpdateMemoryState(true);
		SchedulePendingWrites();
	}
}

void AsyncFileWriter::WritePendingWrites(vector<unique_ptr<AsyncWriteBuffer>> &writes) {
	// The writer is a sequential stream: keep registration order and write through the handle's current position.
	idx_t i = 0;
	while (i < writes.size()) {
		auto &write = *writes[i];
		auto write_size = write.Size();
		if (write_size >= coalesce_threshold) {
			WriteBuffer(write.Ptr(), write_size);
			i++;
			continue;
		}

		idx_t coalesced_size = 0;
		idx_t end = i;
		while (end < writes.size()) {
			auto next_size = writes[end]->Size();
			if (next_size >= coalesce_threshold || coalesced_size + next_size > coalesce_threshold) {
				break;
			}
			coalesced_size += next_size;
			end++;
		}
		if (end == i + 1) {
			WriteBuffer(write.Ptr(), write_size);
			i++;
			continue;
		}

		auto coalesced = BufferAllocator::Get(client_context).Allocate(coalesced_size);
		idx_t offset = 0;
		for (idx_t write_idx = i; write_idx < end; write_idx++) {
			auto &current = *writes[write_idx];
			memcpy(coalesced.get() + offset, current.Ptr(), current.Size());
			offset += current.Size();
		}
		WriteBuffer(coalesced.get(), coalesced_size);
		i = end;
	}
}

void AsyncFileWriter::WriteBuffer(data_ptr_t buffer, idx_t size) {
	handle->Write(context, buffer, size);
}

void AsyncFileWriter::RethrowTaskError() {
	if (executor && executor->HasError()) {
		executor->ThrowError();
	}
}

void AsyncFileWriter::Flush() {
	WaitAll();
}

void AsyncFileWriter::ApplyBackpressure() {
	if (!executor) {
		return;
	}
	RethrowTaskError();
	while (true) {
		idx_t current_pending_bytes;
		{
			lock_guard<mutex> guard(lock);
			if (batch_depth > 0) {
				return;
			}
			current_pending_bytes = pending_bytes;
		}
		if (current_pending_bytes <= BackpressureBudget()) {
			return;
		}
		SchedulePendingWrites();
		executor->WorkOnTasks();
		RethrowTaskError();
	}
}

void AsyncFileWriter::WaitAll() {
	if (executor) {
		{
			lock_guard<mutex> guard(lock);
			batch_depth = 0;
		}
		UpdateMemoryState(true);
		SchedulePendingWrites();
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
