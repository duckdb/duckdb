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

static idx_t CeilDiv(idx_t value, idx_t divisor) {
	D_ASSERT(divisor > 0);
	return value == 0 ? 0 : 1 + (value - 1) / divisor;
}

AsyncFileWriter::PendingWrite::PendingWrite(unique_ptr<AsyncWriteBuffer> buffer_p, idx_t offset_p)
    : buffer(std::move(buffer_p)), offset(offset_p) {
}

idx_t AsyncFileWriter::PendingWrite::Size() const {
	return buffer->Size();
}

class AsyncFileWriterDrainTaskGuard {
public:
	AsyncFileWriterDrainTaskGuard(AsyncFileWriter &writer_p, idx_t in_flight_task_bytes_p)
	    : writer(writer_p), in_flight_task_bytes(in_flight_task_bytes_p) {
	}

	~AsyncFileWriterDrainTaskGuard() {
		Finish();
	}

	void Finish() {
		if (!finished) {
			writer.FinishDrainTask(in_flight_task_bytes);
			finished = true;
		}
	}

private:
	AsyncFileWriter &writer;
	idx_t in_flight_task_bytes;
	bool finished = false;
};

class AsyncFileWriterTask : public BaseExecutorTask {
public:
	AsyncFileWriterTask(AsyncFileWriter &writer_p, TaskExecutor &executor)
	    : BaseExecutorTask(executor), writer(writer_p) {
	}

	~AsyncFileWriterTask() override {
		if (!started) {
			writer.CancelScheduledDrainTask();
		}
	}

	void ExecuteTask() override {
		started = true;
		writer.StartDrainTask();
		writer.DrainPendingWrites();
	}

private:
	AsyncFileWriter &writer;
	bool started = false;
};

AsyncFileWriter::AsyncFileWriter(QueryContext context_p, FileSystem &fs_p, const string &path_p,
                                 FileOpenFlags open_flags)
    : context(context_p), client_context(RequireClientContext(context_p)), fs(fs_p), path(path_p) {
	handle = fs.OpenFile(path, open_flags | FileLockType::WRITE_LOCK);
	ResolveWriteSettings();
	auto &scheduler = TaskScheduler::GetScheduler(client_context);
	auto async_threads = NumericCast<idx_t>(scheduler.NumberOfAsyncThreads());
	if (async_threads > 0) {
		if (SupportsPositionalWrites()) {
			drain_mode = DrainMode::POSITIONAL;
			max_active_drain_tasks = async_threads;
			if (local_file) {
				auto regular_threads = MaxValue<idx_t>(NumericCast<idx_t>(scheduler.NumberOfThreads()), 1);
				auto local_task_limit =
				    MaxValue<idx_t>(regular_threads / DEFAULT_LOCAL_REGULAR_THREADS_PER_DRAIN_TASK, 1);
				max_active_drain_tasks = MinValue(max_active_drain_tasks, local_task_limit);
			}
		}
		executor = make_uniq<TaskExecutor>(client_context, TaskSchedulerType::ASYNC);
		memory_state = TemporaryMemoryManager::Get(client_context).Register(client_context);
		memory_state->SetMinimumReservation(min_pending_bytes);
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
	local_file = fs.IsLocalFileSystem();
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
	min_pending_bytes = MinValue(max_pending_bytes, DEFAULT_MIN_PENDING_BYTES_PER_THREAD * regular_threads);
}

idx_t AsyncFileWriter::GetFileSize() {
	return GetTotalWritten();
}

idx_t AsyncFileWriter::GetTotalWritten() const {
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
		idx_t sealed_buffer_offset = 0;
		if (!copied_buffer) {
			copied_buffer_offset = total_written;
			copied_buffer = make_uniq<CopiedAsyncWriteBuffer>(client_context, DEFAULT_COPIED_BUFFER_CAPACITY);
		}
		auto append_size = MinValue(write_size - offset, copied_buffer->Remaining());
		copied_buffer->Append(buffer + offset, append_size);
		total_written += append_size;
		offset += append_size;
		if (copied_buffer->Remaining() == 0) {
			sealed_buffer_offset = copied_buffer_offset;
			sealed_buffer = std::move(copied_buffer);
		}
		if (sealed_buffer) {
			RegisterStagedWrite(std::move(sealed_buffer), sealed_buffer_offset);
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

void AsyncFileWriter::RegisterWrite(unique_ptr<AsyncWriteBuffer> buffer, ScheduleMode schedule_mode) {
	RethrowTaskError();
	if (closed) {
		throw IOException("Cannot write to closed file \"%s\"", path);
	}

	auto write_size = buffer->Size();
	auto offset = total_written;
	total_written += write_size;
	RegisterWriteInternal(std::move(buffer), offset, schedule_mode);
}

void AsyncFileWriter::RegisterStagedWrite(unique_ptr<AsyncWriteBuffer> buffer, idx_t offset,
                                          ScheduleMode schedule_mode) {
	RethrowTaskError();
	if (closed) {
		throw IOException("Cannot write to closed file \"%s\"", path);
	}
	RegisterWriteInternal(std::move(buffer), offset, schedule_mode);
}

void AsyncFileWriter::RegisterWriteInternal(unique_ptr<AsyncWriteBuffer> buffer, idx_t offset,
                                            ScheduleMode schedule_mode) {
	auto write_size = buffer->Size();
	if (!executor) {
		WriteBuffer(buffer->Ptr(), write_size, offset);
		return;
	}

	{
		lock_guard<mutex> guard(lock);
		pending_writes.emplace_back(std::move(buffer), offset);
		pending_bytes += write_size;
	}
	UpdateMemoryState();
	if (schedule_mode == ScheduleMode::ALLOW) {
		SchedulePendingWrites();
	}
}

void AsyncFileWriter::SealCopiedBuffer(ScheduleMode schedule_mode) {
	if (!copied_buffer || copied_buffer->Size() == 0) {
		return;
	}
	auto sealed_buffer_offset = copied_buffer_offset;
	auto sealed_buffer = std::move(copied_buffer);
	RegisterStagedWrite(std::move(sealed_buffer), sealed_buffer_offset, schedule_mode);
}

AsyncFileWriter::BatchGuard AsyncFileWriter::StartBatch() {
	return BatchGuard(*this);
}

void AsyncFileWriter::SchedulePendingWrites(SchedulePolicy policy) {
	if (!executor) {
		return;
	}
	SealCopiedBuffer(ScheduleMode::DEFER);
	SchedulePendingWritesInternal(policy);
}

void AsyncFileWriter::SchedulePendingWritesInternal(SchedulePolicy policy) {
	if (!executor) {
		return;
	}
	idx_t schedule_count = 0;
	{
		lock_guard<mutex> guard(lock);
		if (batch_depth == 0 && !pending_writes.empty() && active_drain_tasks < max_active_drain_tasks) {
			auto available_slots = max_active_drain_tasks - active_drain_tasks;
			schedule_count = EstimateScheduleCount(available_slots, policy);
			active_drain_tasks += schedule_count;
			pending_drain_tasks += schedule_count;
		}
	}
	for (idx_t task_idx = 0; task_idx < schedule_count; task_idx++) {
		unique_ptr<AsyncFileWriterTask> task;
		try {
			task = make_uniq<AsyncFileWriterTask>(*this, *executor);
		} catch (...) {
			CancelScheduledDrainTasks(schedule_count - task_idx);
			throw;
		}
		try {
			executor->ScheduleTask(std::move(task));
		} catch (...) {
			// The task destructor releases this task's slot. Release the slots for tasks not yet created.
			CancelScheduledDrainTasks(schedule_count - task_idx - 1);
			throw;
		}
	}
}

void AsyncFileWriter::UpdateMemoryState(MemoryUpdateMode mode) {
	if (!memory_state) {
		return;
	}

	auto force = mode == MemoryUpdateMode::FORCE;
	idx_t current_pending_bytes;
	{
		lock_guard<mutex> guard(lock);
		if (batch_depth > 0 && !force) {
			return;
		}
		current_pending_bytes = TotalPendingBytes();
	}
	if (current_pending_bytes == 0) {
		return;
	}

	auto current_reservation = memory_state->GetReservation();
	while (current_pending_bytes > MinValue(current_reservation, max_pending_bytes)) {
		idx_t next_request;
		if (memory_request_bytes > current_reservation) {
			// TMM did not fully grant the previous request. Keep retrying it on later growth checks.
			next_request = memory_request_bytes;
		} else if (memory_request_bytes == 0) {
			next_request = min_pending_bytes;
		} else if (memory_request_bytes >= max_pending_bytes) {
			return;
		} else if (memory_request_bytes > max_pending_bytes / 2) {
			next_request = max_pending_bytes;
		} else {
			next_request = memory_request_bytes * 2;
		}
		next_request = MinValue(MaxValue(next_request, min_pending_bytes), max_pending_bytes);
		if (next_request <= memory_request_bytes) {
			return;
		}

		auto previous_reservation = current_reservation;
		memory_state->SetRemainingSizeAndUpdateReservation(client_context, next_request);
		memory_request_bytes = next_request;
		current_reservation = memory_state->GetReservation();
		if (current_reservation <= previous_reservation) {
			return;
		}
		if (current_reservation < next_request) {
			return;
		}
	}
}

idx_t AsyncFileWriter::BackpressureBudget() {
	if (!memory_state) {
		return NumericLimits<idx_t>::Maximum();
	}
	return MinValue(memory_state->GetReservation(), max_pending_bytes);
}

idx_t AsyncFileWriter::DrainTaskByteBudget() const {
	return MaxValue(DEFAULT_DRAIN_TASK_BYTE_BUDGET, coalesce_threshold);
}

idx_t AsyncFileWriter::TotalPendingBytes() const {
	return pending_bytes + in_flight_bytes;
}

idx_t AsyncFileWriter::UnscheduledPendingBytes() const {
	if (pending_drain_tasks == 0 || pending_bytes == 0) {
		return pending_bytes;
	}
	auto byte_budget = DrainTaskByteBudget();
	auto covered_tasks = CeilDiv(pending_bytes, byte_budget);
	if (pending_drain_tasks >= covered_tasks) {
		return 0;
	}
	return pending_bytes - pending_drain_tasks * byte_budget;
}

idx_t AsyncFileWriter::FirstTaskScheduleThreshold() const {
	return local_file ? 1 : coalesce_threshold;
}

bool AsyncFileWriter::SupportsPositionalWrites() {
	uint8_t empty = 0;
	try {
		handle->Write(context, &empty, 0, 0);
		return true;
	} catch (const NotImplementedException &) {
		return false;
	}
}

idx_t AsyncFileWriter::EstimateScheduleCount(idx_t available_slots, SchedulePolicy policy) const {
	if (available_slots == 0 || pending_writes.empty()) {
		return 0;
	}
	auto unscheduled_bytes = UnscheduledPendingBytes();
	if (unscheduled_bytes == 0) {
		return 0;
	}
	auto byte_budget = DrainTaskByteBudget();
	if (policy == SchedulePolicy::FORCE) {
		auto schedule_count = CeilDiv(unscheduled_bytes, byte_budget);
		return MinValue(schedule_count, available_slots);
	}

	if (active_drain_tasks == 0) {
		if (unscheduled_bytes < FirstTaskScheduleThreshold()) {
			return 0;
		}
		if (drain_mode == DrainMode::SEQUENTIAL) {
			return 1;
		}
		idx_t schedule_count = 1;
		unscheduled_bytes = unscheduled_bytes > byte_budget ? unscheduled_bytes - byte_budget : 0;
		schedule_count += unscheduled_bytes / byte_budget;
		return MinValue(schedule_count, available_slots);
	}

	if (drain_mode == DrainMode::SEQUENTIAL) {
		return 0;
	}

	auto schedule_count = unscheduled_bytes / byte_budget;
	return MinValue(schedule_count, available_slots);
}

void AsyncFileWriter::StartDrainTask() {
	lock_guard<mutex> guard(lock);
	D_ASSERT(pending_drain_tasks > 0);
	pending_drain_tasks--;
}

idx_t AsyncFileWriter::TakePendingWrites(vector<PendingWrite> &writes) {
	lock_guard<mutex> guard(lock);
	if (pending_writes.empty() || batch_depth > 0) {
		return 0;
	}

	auto byte_budget = DrainTaskByteBudget();
	idx_t selected_bytes = 0;
	idx_t end = 0;
	while (end < pending_writes.size()) {
		auto write_size = pending_writes[end].Size();
		if (selected_bytes > 0 && selected_bytes + write_size > byte_budget) {
			break;
		}
		selected_bytes += write_size;
		end++;
		if (selected_bytes >= byte_budget) {
			break;
		}
	}

	writes.reserve(end);
	for (idx_t write_idx = 0; write_idx < end; write_idx++) {
		writes.push_back(std::move(pending_writes[write_idx]));
	}
	pending_writes.erase(pending_writes.begin(), pending_writes.begin() + end);
	D_ASSERT(pending_bytes >= selected_bytes);
	pending_bytes -= selected_bytes;
	in_flight_bytes += selected_bytes;
	return selected_bytes;
}

void AsyncFileWriter::FinishDrainTask(idx_t in_flight_task_bytes) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(active_drain_tasks > 0);
	active_drain_tasks--;
	D_ASSERT(in_flight_bytes >= in_flight_task_bytes);
	in_flight_bytes -= in_flight_task_bytes;
}

void AsyncFileWriter::CancelScheduledDrainTask() {
	CancelScheduledDrainTasks(1);
}

void AsyncFileWriter::CancelScheduledDrainTasks(idx_t task_count) {
	if (task_count == 0) {
		return;
	}
	lock_guard<mutex> guard(lock);
	D_ASSERT(active_drain_tasks >= task_count);
	D_ASSERT(pending_drain_tasks >= task_count);
	active_drain_tasks -= task_count;
	pending_drain_tasks -= task_count;
}

void AsyncFileWriter::DrainPendingWrites() {
	vector<PendingWrite> writes;
	auto in_flight_task_bytes = TakePendingWrites(writes);
	AsyncFileWriterDrainTaskGuard guard(*this, in_flight_task_bytes);
	if (writes.empty()) {
		guard.Finish();
		return;
	}

	auto drained_bytes = WritePendingWrites(writes);
	D_ASSERT(drained_bytes == in_flight_task_bytes);
	guard.Finish();
	SchedulePendingWritesInternal();
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

idx_t AsyncFileWriter::WritePendingWrites(vector<PendingWrite> &writes) {
	idx_t drained_bytes = 0;
	idx_t i = 0;
	auto write_range = [&](idx_t start, idx_t end, idx_t size) {
		D_ASSERT(end > start);
		auto write_offset = writes[start].offset;
		if (end == start + 1) {
			auto &single_write = *writes[start].buffer;
			D_ASSERT(size == single_write.Size());
			WriteBuffer(single_write.Ptr(), size, write_offset);
			writes[start].buffer.reset();
			return size;
		}

		auto coalesced = BufferAllocator::Get(client_context).Allocate(size);
		idx_t offset = 0;
		for (idx_t write_idx = start; write_idx < end; write_idx++) {
			auto &current = writes[write_idx];
			auto current_size = current.Size();
			D_ASSERT(current.offset == write_offset + offset);
			memcpy(coalesced.get() + offset, current.buffer->Ptr(), current_size);
			offset += current_size;
			// The coalesced buffer owns these bytes now; release the sources before the physical write.
			current.buffer.reset();
		}
		D_ASSERT(offset == size);
		WriteBuffer(coalesced.get(), size, write_offset);
		return size;
	};

	while (i < writes.size()) {
		auto &pending_write = writes[i];
		auto &write = *pending_write.buffer;
		auto write_size = write.Size();
		if (write_size >= coalesce_threshold) {
			drained_bytes += write_range(i, i + 1, write_size);
			i++;
			continue;
		}

		idx_t coalesced_size = 0;
		idx_t end = i;
		if (local_file) {
			while (end < writes.size()) {
				auto next_size = writes[end].Size();
				if (next_size >= coalesce_threshold || coalesced_size + next_size > coalesce_threshold) {
					break;
				}
				D_ASSERT(writes[end].offset == pending_write.offset + coalesced_size);
				coalesced_size += next_size;
				end++;
			}
			drained_bytes += write_range(i, end, coalesced_size);
			i = end;
			continue;
		}

		while (end < writes.size() && writes[end].Size() < coalesce_threshold) {
			D_ASSERT(writes[end].offset == pending_write.offset + coalesced_size);
			coalesced_size += writes[end].Size();
			end++;
		}
		idx_t remaining_size = coalesced_size;
		idx_t chunk_start = i;
		while (chunk_start < end) {
			idx_t chunk_size = 0;
			idx_t chunk_end = chunk_start;
			while (chunk_end < end) {
				auto next_size = writes[chunk_end].Size();
				chunk_size += next_size;
				chunk_end++;
				auto remaining_after_next = remaining_size - chunk_size;
				if (chunk_size >= coalesce_threshold &&
				    (remaining_after_next == 0 || remaining_after_next >= coalesce_threshold)) {
					break;
				}
			}
			drained_bytes += write_range(chunk_start, chunk_end, chunk_size);
			remaining_size -= chunk_size;
			chunk_start = chunk_end;
		}
		i = end;
	}
	return drained_bytes;
}

void AsyncFileWriter::WriteBuffer(data_ptr_t buffer, idx_t size, idx_t offset) {
	if (size == 0) {
		return;
	}
	if (drain_mode == DrainMode::POSITIONAL) {
		handle->Write(context, buffer, size, offset);
	} else {
		handle->Write(context, buffer, size);
	}
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
	if (batch_depth > 0) {
		return;
	}
	SealCopiedBuffer(ScheduleMode::DEFER);
	UpdateMemoryState(MemoryUpdateMode::FORCE);
	while (true) {
		idx_t current_pending_bytes;
		{
			lock_guard<mutex> guard(lock);
			if (batch_depth > 0) {
				return;
			}
			current_pending_bytes = TotalPendingBytes();
		}
		if (current_pending_bytes <= BackpressureBudget()) {
			return;
		}
		SchedulePendingWrites(SchedulePolicy::FORCE);
		executor->WorkOnTasks();
		RethrowTaskError();
	}
}

void AsyncFileWriter::WaitAll() {
	WaitAllInternal(BatchDrainMode::PRESERVE_BATCH);
}

void AsyncFileWriter::WaitAllInternal(BatchDrainMode batch_drain_mode) {
	if (!executor) {
		SealCopiedBuffer(ScheduleMode::DEFER);
		RethrowTaskError();
		return;
	}

	const auto preserve_batch = batch_drain_mode == BatchDrainMode::PRESERVE_BATCH;
	idx_t previous_batch_depth = 0;
	bool batch_opened_for_drain = false;

	auto open_batch_for_drain = [&]() {
		if (batch_opened_for_drain) {
			return;
		}
		lock_guard<mutex> guard(lock);
		previous_batch_depth = batch_depth;
		batch_depth = 0;
		batch_opened_for_drain = true;
	};
	auto restore_batch = [&]() {
		if (!preserve_batch || !batch_opened_for_drain) {
			return;
		}
		lock_guard<mutex> guard(lock);
		batch_depth = previous_batch_depth;
	};

	try {
		if (!executor->HasError()) {
			SealCopiedBuffer(ScheduleMode::DEFER);
		}
		open_batch_for_drain();
		UpdateMemoryState(MemoryUpdateMode::FORCE);
		if (!executor->HasError()) {
			SchedulePendingWritesInternal(SchedulePolicy::FORCE);
		}
		executor->WorkOnTasks();
	} catch (...) {
		try {
			open_batch_for_drain();
			executor->WorkOnTasks();
		} catch (...) {
		}
		restore_batch();
		throw;
	}

	restore_batch();
	RethrowTaskError();
}

void AsyncFileWriter::ReleaseMemoryReservation() {
	if (!memory_state || memory_request_bytes == 0) {
		return;
	}
	memory_state->SetZero();
	memory_request_bytes = 0;
}

void AsyncFileWriter::Close() {
	if (closed) {
		return;
	}
	try {
		WaitAllInternal(BatchDrainMode::FORCE_CLOSE_BATCH);
		ReleaseMemoryReservation();
		handle->Close();
		handle.reset();
		closed = true;
	} catch (...) {
		ReleaseMemoryReservation();
		throw;
	}
}

void AsyncFileWriter::Sync() {
	WaitAll();
	handle->Sync();
}

void AsyncFileWriter::Truncate(idx_t size) {
	WaitAll();
	handle->Truncate(NumericCast<int64_t>(size));
	total_written = size;
	if (handle->CanSeek() && handle->SeekPosition() > size) {
		handle->Seek(size);
	}
}

} // namespace duckdb
