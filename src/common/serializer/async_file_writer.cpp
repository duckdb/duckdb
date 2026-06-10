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

class CopiedAsyncWriteBuffer : public AsyncWriteBuffer {
public:
	CopiedAsyncWriteBuffer(ClientContext &context, idx_t capacity_p)
	    : data(BufferAllocator::Get(context).Allocate(capacity_p)), capacity(capacity_p) {
	}

	data_ptr_t Ptr() override {
		return data.get();
	}

	idx_t Size() const override {
		return size;
	}

	idx_t Remaining() const {
		return capacity - size;
	}

	void Append(const_data_ptr_t buffer, idx_t append_size) {
		D_ASSERT(append_size <= Remaining());
		memcpy(data.get() + size, buffer, append_size);
		size += append_size;
	}

private:
	AllocatedData data;
	idx_t capacity;
	idx_t size = 0;
};

static ClientContext &RequireClientContext(QueryContext context) {
	auto client_context = context.GetClientContext();
	if (!client_context) {
		throw InvalidInputException("AsyncFileWriter requires a ClientContext");
	}
	return *client_context;
}

static idx_t MemoryStateUpdateGranularity(idx_t max_pending_bytes, idx_t coalesce_threshold) {
	constexpr idx_t MINIMUM_UPDATE_GRANULARITY = 1024ULL * 1024ULL;
	auto threshold = max_pending_bytes / 16;
	threshold = MaxValue(threshold, coalesce_threshold);
	return MaxValue(threshold, MINIMUM_UPDATE_GRANULARITY);
}

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
                                 FileOpenFlags open_flags)
    : context(context_p), client_context(RequireClientContext(context_p)), fs(fs_p), path(path_p) {
	handle = fs.OpenFile(path, open_flags | FileLockType::WRITE_LOCK);
	ResolveWriteSettings();
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

void AsyncFileWriter::ResolveWriteSettings() {
	bool local_file = fs.IsLocalFileSystem();
	if (!local_file && handle) {
		try {
			local_file = handle->OnDiskFile();
		} catch (...) {
			local_file = false;
		}
	}

	coalesce_threshold = local_file ? DEFAULT_LOCAL_COALESCE_THRESHOLD : DEFAULT_REMOTE_COALESCE_THRESHOLD;

	auto &scheduler = TaskScheduler::GetScheduler(client_context);
	auto regular_threads = MaxValue<idx_t>(NumericCast<idx_t>(scheduler.NumberOfThreads()), 1);
	max_pending_bytes = DEFAULT_MAX_PENDING_BYTES_PER_THREAD * regular_threads;
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
	RethrowTaskError();
	if (closed) {
		throw IOException("Cannot write to closed file \"%s\"", path);
	}

	if (write_size >= DEFAULT_COPIED_BUFFER_CAPACITY) {
		SealCopiedBuffer(ScheduleMode::DEFER);
		auto owned_buffer = make_uniq<CopiedAsyncWriteBuffer>(client_context, write_size);
		owned_buffer->Append(buffer, write_size);
		RegisterWrite(std::move(owned_buffer));
		return;
	}

	idx_t offset = 0;
	while (offset < write_size) {
		unique_ptr<CopiedAsyncWriteBuffer> sealed_buffer;
		{
			lock_guard<mutex> guard(lock);
			if (!copied_buffer) {
				copied_buffer = make_uniq<CopiedAsyncWriteBuffer>(client_context, DEFAULT_COPIED_BUFFER_CAPACITY);
			}
			auto append_size = MinValue(write_size - offset, copied_buffer->Remaining());
			copied_buffer->Append(buffer + offset, append_size);
			total_written += append_size;
			offset += append_size;
			if (copied_buffer->Remaining() == 0) {
				sealed_buffer = std::move(copied_buffer);
			}
		}
		if (sealed_buffer) {
			RegisterWrite(std::move(sealed_buffer), WriteAccounting::ALREADY_COUNTED);
		}
	}
}

void AsyncFileWriter::WriteData(unique_ptr<AsyncWriteBuffer> buffer) {
	if (!buffer || buffer->Size() == 0) {
		return;
	}
	SealCopiedBuffer(ScheduleMode::DEFER);
	RegisterWrite(std::move(buffer));
}

void AsyncFileWriter::RegisterWrite(unique_ptr<AsyncWriteBuffer> buffer, WriteAccounting accounting,
                                    ScheduleMode schedule_mode) {
	RethrowTaskError();
	if (closed) {
		throw IOException("Cannot write to closed file \"%s\"", path);
	}

	auto write_size = buffer->Size();
	auto update_total_written = accounting == WriteAccounting::ADD_TO_TOTAL_WRITTEN;
	if (!executor) {
		WriteBuffer(buffer->Ptr(), write_size);
		if (update_total_written) {
			lock_guard<mutex> guard(lock);
			total_written += write_size;
		}
		return;
	}

	bool should_schedule = false;
	{
		lock_guard<mutex> guard(lock);
		pending_bytes += write_size;
		if (update_total_written) {
			total_written += write_size;
		}
		pending_writes.push_back(std::move(buffer));
		if (schedule_mode == ScheduleMode::ALLOW && !task_scheduled && batch_depth == 0) {
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

void AsyncFileWriter::SealCopiedBuffer(ScheduleMode schedule_mode) {
	unique_ptr<CopiedAsyncWriteBuffer> sealed_buffer;
	{
		lock_guard<mutex> guard(lock);
		if (!copied_buffer || copied_buffer->Size() == 0) {
			return;
		}
		sealed_buffer = std::move(copied_buffer);
	}
	RegisterWrite(std::move(sealed_buffer), WriteAccounting::ALREADY_COUNTED, schedule_mode);
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
	SealCopiedBuffer(ScheduleMode::DEFER);
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

void AsyncFileWriter::UpdateMemoryState(MemoryUpdateMode mode) {
	if (!memory_state) {
		return;
	}

	auto force = mode == MemoryUpdateMode::FORCE;
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
		auto drained_bytes = WritePendingWrites(writes);
		{
			lock_guard<mutex> guard(lock);
			D_ASSERT(pending_bytes >= drained_bytes);
			pending_bytes -= drained_bytes;
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
		UpdateMemoryState(MemoryUpdateMode::FORCE);
		SchedulePendingWrites();
	}
}

idx_t AsyncFileWriter::WritePendingWrites(vector<unique_ptr<AsyncWriteBuffer>> &writes) {
	// The writer is a sequential stream: keep registration order and write through the handle's current position.
	idx_t drained_bytes = 0;
	idx_t i = 0;
	while (i < writes.size()) {
		auto &write = *writes[i];
		auto write_size = write.Size();
		if (write_size >= coalesce_threshold) {
			WriteBuffer(write.Ptr(), write_size);
			drained_bytes += write_size;
			writes[i].reset();
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
			drained_bytes += write_size;
			writes[i].reset();
			i++;
			continue;
		}

		auto coalesced = BufferAllocator::Get(client_context).Allocate(coalesced_size);
		idx_t offset = 0;
		for (idx_t write_idx = i; write_idx < end; write_idx++) {
			auto &current = writes[write_idx];
			auto current_size = current->Size();
			memcpy(coalesced.get() + offset, current->Ptr(), current_size);
			offset += current_size;
			// The coalesced buffer owns these bytes now; release the sources before the physical write.
			current.reset();
		}
		D_ASSERT(offset == coalesced_size);
		WriteBuffer(coalesced.get(), coalesced_size);
		drained_bytes += coalesced_size;
		i = end;
	}
	return drained_bytes;
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
	{
		lock_guard<mutex> guard(lock);
		if (batch_depth > 0) {
			return;
		}
	}
	SealCopiedBuffer(ScheduleMode::DEFER);
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
	SealCopiedBuffer(ScheduleMode::DEFER);
	if (executor) {
		{
			lock_guard<mutex> guard(lock);
			batch_depth = 0;
		}
		UpdateMemoryState(MemoryUpdateMode::FORCE);
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
