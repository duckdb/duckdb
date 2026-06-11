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
		// Positional writes let multiple async tasks drain one logical stream concurrently.
		// Otherwise the writer keeps one sequential drain task active so file handle ordering remains correct.
		if (SupportsPositionalWrites()) {
			drain_mode = DrainMode::POSITIONAL;
			max_active_drain_tasks = async_threads;
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
	// We would call Finish() here, but that can throw, instead we assert it has been called.
	D_ASSERT(Exception::UncaughtException() || !writer);
	if (writer) {
		writer->LeaveBatch();
	}
}

void AsyncFileWriter::BatchGuard::Finish() {
	if (!writer) {
		return;
	}
	auto &writer_ref = *writer;
	writer = nullptr;
	auto apply_backpressure = !writer_ref.closed;
	writer_ref.LeaveBatch();
	if (apply_backpressure) {
		writer_ref.ApplyBackpressure();
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

	// Caller-owned memory cannot outlive async scheduling, so even large const inputs are copied before registration.
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
	RethrowTaskError();
	if (closed) {
		throw IOException("Cannot write to closed file \"%s\"", path);
	}
	if (!executor) {
		// Keep the no-async path buffered like BufferedFileWriter instead of turning every owned buffer into a syscall.
		WriteDataSynchronously(buffer->Ptr(), buffer->Size());
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

void AsyncFileWriter::WriteDataSynchronously(data_ptr_t buffer, idx_t write_size) {
	auto copied_size = copied_buffer ? copied_buffer->Size() : 0;
	if (write_size >= 2 * DEFAULT_COPIED_BUFFER_CAPACITY - copied_size) {
		idx_t copied_prefix = 0;
		if (copied_size > 0) {
			copied_prefix = copied_buffer->Remaining();
			D_ASSERT(copied_prefix <= write_size);
			copied_buffer->Append(buffer, copied_prefix);
			total_written += copied_prefix;
			SealCopiedBuffer(ScheduleMode::DEFER);
		}
		auto remaining_size = write_size - copied_prefix;
		if (remaining_size > 0) {
			auto offset = total_written;
			total_written += remaining_size;
			WriteBuffer(buffer + copied_prefix, remaining_size, offset);
		}
		return;
	}

	idx_t input_offset = 0;
	while (input_offset < write_size) {
		unique_ptr<CopiedAsyncWriteBuffer> sealed_buffer;
		idx_t sealed_buffer_offset = 0;
		if (!copied_buffer) {
			copied_buffer_offset = total_written;
			copied_buffer = make_uniq<CopiedAsyncWriteBuffer>(client_context, DEFAULT_COPIED_BUFFER_CAPACITY);
		}
		auto append_size = MinValue(write_size - input_offset, copied_buffer->Remaining());
		copied_buffer->Append(buffer + input_offset, append_size);
		total_written += append_size;
		input_offset += append_size;
		if (copied_buffer->Remaining() == 0) {
			sealed_buffer_offset = copied_buffer_offset;
			sealed_buffer = std::move(copied_buffer);
		}
		if (sealed_buffer) {
			RegisterStagedWrite(std::move(sealed_buffer), sealed_buffer_offset);
		}
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
			// Reserve task slots before ScheduleTask().
			// The task destructor releases a slot if scheduling fails before ExecuteTask() starts.
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
			// Grow coarsely and only release on Close().
			// Repeatedly shrinking here would touch shared TMM state on the row-group hot path.
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

idx_t AsyncFileWriter::SelectPendingWriteEnd(idx_t start, idx_t &selected_bytes) const {
	D_ASSERT(start < pending_writes.size());
	auto byte_budget = DrainTaskByteBudget();
	selected_bytes = 0;
	idx_t end = start;
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
	D_ASSERT(end > start);
	return end;
}

idx_t AsyncFileWriter::FirstUnscheduledPendingWrite(idx_t &scheduled_bytes) const {
	idx_t write_idx = 0;
	scheduled_bytes = 0;
	for (idx_t task_idx = 0; task_idx < pending_drain_tasks && write_idx < pending_writes.size(); task_idx++) {
		idx_t selected_bytes;
		write_idx = SelectPendingWriteEnd(write_idx, selected_bytes);
		scheduled_bytes += selected_bytes;
	}
	D_ASSERT(scheduled_bytes <= pending_bytes);
	return write_idx;
}

idx_t AsyncFileWriter::CountPendingWriteTasks(idx_t start_write, idx_t available_slots,
                                              PendingTaskCountMode mode) const {
	idx_t task_count = 0;
	auto byte_budget = DrainTaskByteBudget();
	while (available_slots > 0 && start_write < pending_writes.size()) {
		idx_t selected_bytes;
		auto next_write = SelectPendingWriteEnd(start_write, selected_bytes);
		if (mode == PendingTaskCountMode::FULL_BUDGET_ONLY && selected_bytes < byte_budget) {
			break;
		}
		task_count++;
		available_slots--;
		start_write = next_write;
	}
	return task_count;
}

idx_t AsyncFileWriter::FirstTaskScheduleThreshold() const {
	return local_file ? 1 : coalesce_threshold;
}

bool AsyncFileWriter::SupportsPositionalWrites() {
	return handle->SupportsPositionalWrites();
}

idx_t AsyncFileWriter::EstimateScheduleCount(idx_t available_slots, SchedulePolicy policy) const {
	if (available_slots == 0 || pending_writes.empty()) {
		return 0;
	}
	auto original_available_slots = available_slots;
	idx_t scheduled_bytes;
	auto start_write = FirstUnscheduledPendingWrite(scheduled_bytes);
	if (start_write >= pending_writes.size()) {
		return 0;
	}
	if (policy == SchedulePolicy::FORCE) {
		return CountPendingWriteTasks(start_write, available_slots, PendingTaskCountMode::INCLUDE_TAIL);
	}

	if (active_drain_tasks == 0) {
		auto unscheduled_bytes = pending_bytes - scheduled_bytes;
		if (unscheduled_bytes < FirstTaskScheduleThreshold()) {
			return 0;
		}
		if (drain_mode == DrainMode::SEQUENTIAL) {
			return 1;
		}
		idx_t schedule_count = 1;
		idx_t selected_bytes;
		start_write = SelectPendingWriteEnd(start_write, selected_bytes);
		available_slots--;
		schedule_count += CountPendingWriteTasks(start_write, available_slots, PendingTaskCountMode::FULL_BUDGET_ONLY);
		return MinValue(schedule_count, original_available_slots);
	}

	if (drain_mode == DrainMode::SEQUENTIAL) {
		return 0;
	}

	return CountPendingWriteTasks(start_write, available_slots, PendingTaskCountMode::FULL_BUDGET_ONLY);
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

	idx_t selected_bytes = 0;
	auto end = SelectPendingWriteEnd(0, selected_bytes);

	writes.reserve(end);
	for (idx_t write_idx = 0; write_idx < end; write_idx++) {
		writes.push_back(std::move(pending_writes[write_idx]));
	}
	auto erase_end = pending_writes.begin() + NumericCast<vector<PendingWrite>::difference_type>(end);
	pending_writes.erase(pending_writes.begin(), erase_end);
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

void AsyncFileWriter::LeaveBatch() noexcept {
	if (!executor) {
		return;
	}
	lock_guard<mutex> guard(lock);
	if (batch_depth == 0) {
		return;
	}
	batch_depth--;
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

void AsyncFileWriter::WorkOnSingleTask() {
	shared_ptr<Task> task;
	if (!executor->GetTask(task)) {
		TaskScheduler::YieldThread();
		return;
	}
	auto result = task->Execute(TaskExecutionMode::PROCESS_ALL);
	D_ASSERT(result != TaskExecutionResult::TASK_BLOCKED);
	task.reset();
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
	SchedulePendingWrites();
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
		WorkOnSingleTask();
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

	// Flush/Close must drain registered writes even if the caller currently has scheduling batched off.
	// Flush restores that batch state, while Close intentionally leaves it closed.
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
