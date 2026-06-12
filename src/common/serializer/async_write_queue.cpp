#include "duckdb/common/serializer/async_write_queue.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

#include <cstring>

namespace duckdb {

AsyncWriteQueue::PendingWrite::PendingWrite(unique_ptr<AsyncWritePayload> payload_p, idx_t offset_p)
    : payload(std::move(payload_p)), offset(offset_p) {
}

idx_t AsyncWriteQueue::PendingWrite::Size() const {
	return payload->Size();
}

class AsyncWriteQueueDrainTaskGuard {
public:
	explicit AsyncWriteQueueDrainTaskGuard(AsyncWriteQueue &queue_p) : queue(queue_p) {
	}

	~AsyncWriteQueueDrainTaskGuard() {
		Finish();
	}

	void SetInFlightBytes(idx_t in_flight_task_bytes_p) {
		D_ASSERT(!finished);
		in_flight_task_bytes = in_flight_task_bytes_p;
	}

	void Finish() {
		if (!finished) {
			queue.FinishDrainTask(in_flight_task_bytes);
			finished = true;
		}
	}

private:
	AsyncWriteQueue &queue;
	idx_t in_flight_task_bytes = 0;
	bool finished = false;
};

class AsyncWriteQueueTask : public BaseExecutorTask {
public:
	AsyncWriteQueueTask(AsyncWriteQueue &queue_p, TaskExecutor &executor) : BaseExecutorTask(executor), queue(queue_p) {
	}

	~AsyncWriteQueueTask() override {
		if (!started) {
			queue.CancelScheduledDrainTask();
		}
	}

	void ExecuteTask() override {
		started = true;
		queue.DrainPendingWrites();
	}

private:
	AsyncWriteQueue &queue;
	bool started = false;
};

AsyncWriteQueue::AsyncWriteQueue(QueryContext context_p, ClientContext &client_context_p, AsyncWriteTarget &target_p,
                                 Options options_p, idx_t async_threads)
    : context(context_p), client_context(client_context_p), target(target_p), options(options_p) {
	options.drain_task_byte_budget = MaxValue(options.drain_task_byte_budget, options.coalesce_threshold);
	options.max_active_drain_tasks = MaxValue<idx_t>(options.max_active_drain_tasks, 1);
	if (options.max_pending_bytes > 0) {
		options.max_pending_bytes = MaxValue(options.max_pending_bytes, options.min_pending_bytes);
		options.min_pending_bytes = MinValue(options.max_pending_bytes, MaxValue<idx_t>(options.min_pending_bytes, 1));
	} else {
		options.min_pending_bytes = 0;
	}

	// Positional writes let multiple async tasks drain one logical write queue concurrently.
	// Otherwise the queue keeps one sequential drain task active so target ordering remains correct.
	if (target.SupportsPositionalWrites()) {
		drain_mode = DrainMode::POSITIONAL;
		max_active_drain_tasks = options.max_active_drain_tasks;
	}

	if (async_threads == 0) {
		return;
	}

	executor = make_uniq<TaskExecutor>(client_context, TaskSchedulerType::ASYNC);
	if (options.max_pending_bytes > 0) {
		memory_state = TemporaryMemoryManager::Get(client_context).Register(client_context);
		memory_state->SetMinimumReservation(options.min_pending_bytes);
		memory_state->SetZero();
	}
}

AsyncWriteQueue::~AsyncWriteQueue() {
	lock_guard<mutex> guard(lock);
	auto drained = batch_depth == 0 && pending_writes.empty() && pending_bytes == 0 && in_flight_bytes == 0 &&
	               active_drain_tasks == 0 && pending_drain_tasks == 0;
	D_ASSERT(closed || drained);
	D_ASSERT(!closed || drained);
}

bool AsyncWriteQueue::IsAsync() const {
	return executor != nullptr;
}

bool AsyncWriteQueue::HasError() {
	return executor && executor->HasError();
}

void AsyncWriteQueue::RegisterWrite(unique_ptr<AsyncWritePayload> payload, idx_t offset, ScheduleMode schedule_mode) {
	if (!payload || payload->Size() == 0) {
		return;
	}
	RethrowTaskError();

	auto write_size = payload->Size();
	if (!executor) {
		VerifyOpen();
		auto next_offset = ValidateRegistrationOffset(offset, write_size);
		WriteBuffer(payload->Ptr(), write_size, offset);
		next_registration_offset = next_offset;
		return;
	}

	{
		lock_guard<mutex> guard(lock);
		VerifyOpen();
		auto next_offset = ValidateRegistrationOffset(offset, write_size);
		pending_writes.emplace_back(std::move(payload), offset);
		pending_bytes += write_size;
		next_registration_offset = next_offset;
	}
	UpdateMemoryState();
	if (schedule_mode == ScheduleMode::ALLOW) {
		SchedulePendingWrites();
	}
}

void AsyncWriteQueue::BeginBatch() {
	if (!executor) {
		VerifyOpen();
		return;
	}
	lock_guard<mutex> guard(lock);
	VerifyOpen();
	batch_depth++;
}

void AsyncWriteQueue::LeaveBatch() noexcept {
	if (!executor) {
		return;
	}
	lock_guard<mutex> guard(lock);
	if (batch_depth == 0) {
		return;
	}
	batch_depth--;
}

bool AsyncWriteQueue::HasOpenBatch() {
	if (!executor) {
		return false;
	}
	lock_guard<mutex> guard(lock);
	return batch_depth > 0;
}

void AsyncWriteQueue::SchedulePendingWrites(SchedulePolicy policy) {
	if (!executor) {
		VerifyOpen();
		return;
	}
	SchedulePendingWritesInternal(policy);
}

void AsyncWriteQueue::SchedulePendingWritesInternal(SchedulePolicy policy) {
	if (!executor) {
		return;
	}
	idx_t schedule_count = 0;
	{
		lock_guard<mutex> guard(lock);
		VerifyOpen();
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
		unique_ptr<AsyncWriteQueueTask> task;
		try {
			task = make_uniq<AsyncWriteQueueTask>(*this, *executor);
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

void AsyncWriteQueue::UpdateMemoryState(MemoryUpdateMode mode) {
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
	while (current_pending_bytes > MinValue(current_reservation, options.max_pending_bytes)) {
		idx_t next_request;
		if (memory_request_bytes > current_reservation) {
			// TMM did not fully grant the previous request. Keep retrying it on later growth checks.
			next_request = memory_request_bytes;
		} else if (memory_request_bytes == 0) {
			// Grow coarsely and only release on Close().
			// Repeatedly shrinking here would touch shared TMM state on the write-registration hot path.
			next_request = options.min_pending_bytes;
		} else if (memory_request_bytes >= options.max_pending_bytes) {
			return;
		} else if (memory_request_bytes > options.max_pending_bytes / 2) {
			next_request = options.max_pending_bytes;
		} else {
			next_request = memory_request_bytes * 2;
		}
		next_request = MinValue(MaxValue(next_request, options.min_pending_bytes), options.max_pending_bytes);
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

idx_t AsyncWriteQueue::BackpressureBudget() {
	if (!memory_state) {
		return NumericLimits<idx_t>::Maximum();
	}
	return MinValue(memory_state->GetReservation(), options.max_pending_bytes);
}

idx_t AsyncWriteQueue::DrainTaskByteBudget() const {
	return MaxValue(options.drain_task_byte_budget, options.coalesce_threshold);
}

idx_t AsyncWriteQueue::TotalPendingBytes() const {
	return pending_bytes + in_flight_bytes;
}

idx_t AsyncWriteQueue::SelectPendingWriteEnd(idx_t start, idx_t &selected_bytes) const {
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

idx_t AsyncWriteQueue::FirstUnscheduledPendingWrite(idx_t &scheduled_bytes) const {
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

idx_t AsyncWriteQueue::CountPendingWriteTasks(idx_t start_write, idx_t available_slots,
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

idx_t AsyncWriteQueue::EstimateScheduleCount(idx_t available_slots, SchedulePolicy policy) const {
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
		if (unscheduled_bytes < options.first_task_schedule_threshold) {
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

idx_t AsyncWriteQueue::TakePendingWrites(vector<PendingWrite> &writes) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(active_drain_tasks > 0);
	D_ASSERT(pending_drain_tasks > 0);
	pending_drain_tasks--;
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

void AsyncWriteQueue::FinishDrainTask(idx_t in_flight_task_bytes) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(active_drain_tasks > 0);
	active_drain_tasks--;
	D_ASSERT(in_flight_bytes >= in_flight_task_bytes);
	in_flight_bytes -= in_flight_task_bytes;
}

void AsyncWriteQueue::CancelScheduledDrainTask() {
	CancelScheduledDrainTasks(1);
}

void AsyncWriteQueue::CancelScheduledDrainTasks(idx_t task_count) {
	if (task_count == 0) {
		return;
	}
	lock_guard<mutex> guard(lock);
	D_ASSERT(active_drain_tasks >= task_count);
	D_ASSERT(pending_drain_tasks >= task_count);
	active_drain_tasks -= task_count;
	pending_drain_tasks -= task_count;
}

void AsyncWriteQueue::DrainPendingWrites() {
	vector<PendingWrite> writes;
	// Claiming writes can allocate. Once a scheduled task has started, always release its active slot.
	AsyncWriteQueueDrainTaskGuard guard(*this);
	auto in_flight_task_bytes = TakePendingWrites(writes);
	guard.SetInFlightBytes(in_flight_task_bytes);
	if (writes.empty()) {
		guard.Finish();
		return;
	}

	auto drained_bytes = WritePendingWrites(writes);
	D_ASSERT(drained_bytes == in_flight_task_bytes);
	guard.Finish();
	SchedulePendingWritesInternal();
}

idx_t AsyncWriteQueue::WritePendingWrites(vector<PendingWrite> &writes) {
	if (options.coalesce_threshold == 0) {
		return WriteDirectPendingWrites(writes);
	}
	if (options.limit_coalesced_write_size) {
		return WriteBoundedCoalescedPendingWrites(writes);
	}
	return WriteThresholdCoalescedPendingWrites(writes);
}

idx_t AsyncWriteQueue::WritePayloadRange(vector<PendingWrite> &writes, idx_t start, idx_t end, idx_t size) {
	D_ASSERT(end > start);
	auto write_offset = writes[start].offset;
	if (end == start + 1) {
		auto &single_write = *writes[start].payload;
		D_ASSERT(size == single_write.Size());
		WriteBuffer(single_write.Ptr(), size, write_offset);
		writes[start].payload.reset();
		return size;
	}

	auto coalesced = BufferAllocator::Get(client_context).Allocate(size);
	idx_t offset = 0;
	for (idx_t write_idx = start; write_idx < end; write_idx++) {
		auto &current = writes[write_idx];
		auto current_size = current.Size();
		VerifyContiguousWrite(current, write_offset + offset);
		memcpy(coalesced.get() + offset, current.payload->Ptr(), current_size);
		offset += current_size;
		// The coalesced buffer owns these bytes now; release the sources before the physical write.
		current.payload.reset();
	}
	D_ASSERT(offset == size);
	WriteBuffer(coalesced.get(), size, write_offset);
	return size;
}

idx_t AsyncWriteQueue::WriteDirectPendingWrites(vector<PendingWrite> &writes) {
	idx_t drained_bytes = 0;
	for (idx_t write_idx = 0; write_idx < writes.size(); write_idx++) {
		drained_bytes += WritePayloadRange(writes, write_idx, write_idx + 1, writes[write_idx].Size());
	}
	return drained_bytes;
}

idx_t AsyncWriteQueue::WriteBoundedCoalescedPendingWrites(vector<PendingWrite> &writes) {
	idx_t drained_bytes = 0;
	idx_t i = 0;
	while (i < writes.size()) {
		auto write_size = writes[i].Size();
		if (write_size >= options.coalesce_threshold) {
			drained_bytes += WritePayloadRange(writes, i, i + 1, write_size);
			i++;
			continue;
		}

		auto write_offset = writes[i].offset;
		idx_t coalesced_size = 0;
		idx_t end = i;
		while (end < writes.size()) {
			auto next_size = writes[end].Size();
			if (next_size >= options.coalesce_threshold ||
			    coalesced_size + next_size > options.coalesce_threshold) {
				break;
			}
			VerifyContiguousWrite(writes[end], write_offset + coalesced_size);
			coalesced_size += next_size;
			end++;
		}
		drained_bytes += WritePayloadRange(writes, i, end, coalesced_size);
		i = end;
	}
	return drained_bytes;
}

idx_t AsyncWriteQueue::WriteThresholdCoalescedPendingWrites(vector<PendingWrite> &writes) {
	idx_t drained_bytes = 0;
	idx_t i = 0;
	while (i < writes.size()) {
		auto write_size = writes[i].Size();
		if (write_size >= options.coalesce_threshold) {
			drained_bytes += WritePayloadRange(writes, i, i + 1, write_size);
			i++;
			continue;
		}

		auto write_offset = writes[i].offset;
		idx_t coalesced_size = 0;
		idx_t end = i;
		while (end < writes.size() && writes[end].Size() < options.coalesce_threshold) {
			VerifyContiguousWrite(writes[end], write_offset + coalesced_size);
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
				if (chunk_size >= options.coalesce_threshold &&
				    (remaining_after_next == 0 || remaining_after_next >= options.coalesce_threshold)) {
					break;
				}
			}
			drained_bytes += WritePayloadRange(writes, chunk_start, chunk_end, chunk_size);
			remaining_size -= chunk_size;
			chunk_start = chunk_end;
		}
		i = end;
	}
	return drained_bytes;
}

void AsyncWriteQueue::WriteBuffer(data_ptr_t buffer, idx_t size, idx_t offset) {
	if (size == 0) {
		return;
	}
	try {
		if (drain_mode == DrainMode::POSITIONAL) {
			target.Write(context, buffer, size, offset);
		} else {
			target.Write(context, buffer, size);
		}
	} catch (const IOException &ex) {
		throw IOException("Async write failed for range [offset=%llu, size=%llu]: %s", offset, size, ex.what());
	} catch (const HTTPException &ex) {
		throw HTTPException(Exception::ConstructMessage("Async write failed for range [offset=%llu, size=%llu]: %s",
		                                                offset, size, ex.what()));
	}
}

idx_t AsyncWriteQueue::ValidateRegistrationOffset(idx_t offset, idx_t write_size) const {
	if (offset != next_registration_offset) {
		throw InternalException("AsyncWriteQueue only supports contiguous writes: expected offset %llu, got %llu",
		                        next_registration_offset, offset);
	}
	return NextWriteOffset(offset, write_size);
}

void AsyncWriteQueue::VerifyContiguousWrite(const PendingWrite &write, idx_t expected_offset) const {
	if (write.offset != expected_offset) {
		throw InternalException("AsyncWriteQueue only supports contiguous writes: expected offset %llu, got %llu",
		                        expected_offset, write.offset);
	}
}

idx_t AsyncWriteQueue::NextWriteOffset(idx_t offset, idx_t write_size) const {
	if (write_size > NumericLimits<idx_t>::Maximum() - offset) {
		throw InternalException("AsyncWriteQueue write offset overflow");
	}
	return offset + write_size;
}

void AsyncWriteQueue::RethrowTaskError() {
	if (executor && executor->HasError()) {
		executor->ThrowError();
	}
}

void AsyncWriteQueue::WorkOnSingleTask() {
	shared_ptr<Task> task;
	if (!executor->GetTask(task)) {
		TaskScheduler::YieldThread();
		return;
	}
	auto result = task->Execute(TaskExecutionMode::PROCESS_ALL);
	D_ASSERT(result != TaskExecutionResult::TASK_BLOCKED);
	task.reset();
}

void AsyncWriteQueue::ApplyBackpressure() {
	if (!executor) {
		VerifyOpen();
		return;
	}
	RethrowTaskError();
	if (HasOpenBatch()) {
		return;
	}
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

void AsyncWriteQueue::WaitAll(BatchDrainMode batch_drain_mode) {
	{
		lock_guard<mutex> guard(lock);
		if (closed) {
			return;
		}
	}
	if (!executor) {
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

void AsyncWriteQueue::VerifyDrained() const {
	if (batch_depth != 0 || !pending_writes.empty() || pending_bytes != 0 || in_flight_bytes != 0 ||
	    active_drain_tasks != 0 || pending_drain_tasks != 0) {
		throw InternalException("AsyncWriteQueue still owns registered writes");
	}
}

void AsyncWriteQueue::Close() {
	{
		lock_guard<mutex> guard(lock);
		if (closed) {
			return;
		}
	}

	try {
		WaitAll(BatchDrainMode::FORCE_CLOSE_BATCH);
		ReleaseMemoryReservation();
	} catch (...) {
		ReleaseMemoryReservation();
		throw;
	}

	lock_guard<mutex> guard(lock);
	VerifyDrained();
	closed = true;
}

void AsyncWriteQueue::ResetNextOffset(idx_t offset) {
	RethrowTaskError();
	lock_guard<mutex> guard(lock);
	VerifyOpen();
	VerifyDrained();
	next_registration_offset = offset;
}

void AsyncWriteQueue::VerifyOpen() const {
	if (closed) {
		throw InternalException("Cannot use closed AsyncWriteQueue");
	}
}

void AsyncWriteQueue::ReleaseMemoryReservation() {
	if (!memory_state || memory_request_bytes == 0) {
		return;
	}
	memory_state->SetZero();
	memory_request_bytes = 0;
}

} // namespace duckdb
