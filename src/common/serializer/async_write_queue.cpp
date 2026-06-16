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
#include <exception>

namespace duckdb {

static ErrorData ErrorDataFromExceptionPtr(std::exception_ptr error_ptr) {
	try {
		std::rethrow_exception(std::move(error_ptr));
	} catch (const std::exception &ex) {
		return ErrorData(ex);
	} catch (...) { // LCOV_EXCL_START
		return ErrorData("Unknown exception during async write");
	} // LCOV_EXCL_STOP
}

AsyncWriteRequest::AsyncWriteRequest(unique_ptr<AsyncWritePayload> payload_p, idx_t offset_p,
                                     AsyncWriteCompletionCallback completion_p)
    : payload(std::move(payload_p)), offset(offset_p), completion(std::move(completion_p)) {
}

idx_t AsyncWriteRequest::Size() const {
	return payload ? payload->Size() : 0;
}

AsyncWriteQueue::PendingRequest::PendingRequest(AsyncWriteRequest request_p)
    : request(std::move(request_p)), size(request.Size()) {
}

idx_t AsyncWriteQueue::PendingRequest::Size() const {
	return size;
}

class AsyncWriteQueueTaskGuard {
public:
	explicit AsyncWriteQueueTaskGuard(AsyncWriteQueue &queue_p) : queue(queue_p) {
	}

	~AsyncWriteQueueTaskGuard() {
		Finish();
	}

	void SetRequestSize(idx_t request_size_p) {
		D_ASSERT(!finished);
		request_size = request_size_p;
	}

	void Finish() {
		if (!finished) {
			queue.FinishTask(request_size);
			finished = true;
		}
	}

private:
	AsyncWriteQueue &queue;
	idx_t request_size = 0;
	bool finished = false;
};

class AsyncWriteQueueTask : public BaseExecutorTask {
public:
	AsyncWriteQueueTask(AsyncWriteQueue &queue_p, TaskExecutor &executor) : BaseExecutorTask(executor), queue(queue_p) {
	}

	~AsyncWriteQueueTask() override {
		if (!started) {
			queue.CancelScheduledTask();
		}
	}

	void ExecuteTask() override {
		started = true;
		queue.DrainRequests();
	}

private:
	AsyncWriteQueue &queue;
	bool started = false;
};

AsyncWriteQueue::AsyncWriteQueue(ClientContext &client_context_p, AsyncWriteTarget &target_p)
    : client_context(client_context_p), target(target_p) {
	auto &scheduler = TaskScheduler::GetScheduler(client_context);
	auto async_threads = NumericCast<idx_t>(scheduler.NumberOfAsyncThreads());
	max_active_tasks = MaxValue<idx_t>(async_threads, 1);
	if (async_threads == 0) {
		return;
	}
	executor = make_uniq<TaskExecutor>(client_context, TaskSchedulerType::ASYNC);
}

AsyncWriteQueue::~AsyncWriteQueue() {
	lock_guard<mutex> guard(lock);
	auto drained = pending_requests.empty() && pending_bytes == 0 && in_flight_bytes == 0 && active_tasks == 0 &&
	               pending_tasks == 0 && scheduled_pending_bytes == 0 && pending_task_bytes.empty();
	D_ASSERT(closed || drained);
	D_ASSERT(!closed || drained);
}

bool AsyncWriteQueue::IsAsync() const {
	return executor != nullptr;
}

bool AsyncWriteQueue::HasError() {
	return executor && executor->HasError();
}

void AsyncWriteQueue::Submit(AsyncWriteRequest request) {
	if (!request.payload || request.Size() == 0) {
		return;
	}
	auto request_size = request.Size();
	if (executor && executor->HasError()) {
		ErrorData error;
		try {
			executor->ThrowError();
		} catch (const std::exception &ex) {
			error = ErrorData(ex);
		} catch (...) { // LCOV_EXCL_START
			error = ErrorData("Unknown exception during async write");
		} // LCOV_EXCL_STOP
		request.payload.reset();
		CompleteRequest(request, request_size, error);
		error.Throw();
	}
	if (!executor) {
		VerifyOpen();
		WriteRequest(std::move(request));
		return;
	}

	{
		lock_guard<mutex> guard(lock);
		VerifyOpen();
		pending_requests.emplace_back(std::move(request));
		pending_bytes += request_size;
	}
	ScheduleTasksInternal();
}

idx_t AsyncWriteQueue::PendingBytes() {
	lock_guard<mutex> guard(lock);
	return pending_bytes + in_flight_bytes;
}

idx_t AsyncWriteQueue::TaskByteBudget() const {
	return task_byte_budget;
}

idx_t AsyncWriteQueue::SelectPendingRequestBytes(idx_t skip_bytes) const {
	D_ASSERT(skip_bytes <= pending_bytes);
	idx_t skipped_bytes = 0;
	idx_t selected_bytes = 0;
	auto byte_budget = TaskByteBudget();
	D_ASSERT(byte_budget > 0);

	for (auto &request : pending_requests) {
		auto request_size = request.Size();
		if (skipped_bytes < skip_bytes) {
			skipped_bytes += request_size;
			continue;
		}

		if (selected_bytes > 0 && selected_bytes + request_size > byte_budget) {
			break;
		}
		selected_bytes += request_size;
		if (selected_bytes >= byte_budget) {
			break;
		}
	}

	D_ASSERT(skipped_bytes == skip_bytes);
	D_ASSERT(selected_bytes > 0);
	return selected_bytes;
}

void AsyncWriteQueue::ScheduleTasksInternal(bool force) {
	if (!executor) {
		return;
	}
	idx_t schedule_count = 0;
	deque<idx_t> task_bytes;
	{
		lock_guard<mutex> guard(lock);
		VerifyOpen();
		idx_t scheduled_bytes = 0;
		while (scheduled_pending_bytes + scheduled_bytes < pending_bytes &&
		       active_tasks + schedule_count < max_active_tasks) {
			auto selected_bytes = SelectPendingRequestBytes(scheduled_pending_bytes + scheduled_bytes);
			auto task_budget = TaskByteBudget();
			auto has_active_task = active_tasks + schedule_count > 0;
			if (!force && has_active_task && selected_bytes < task_budget) {
				break;
			}
			schedule_count++;
			scheduled_bytes += selected_bytes;
			task_bytes.push_back(selected_bytes);
		}
		active_tasks += schedule_count;
		pending_tasks += schedule_count;
		scheduled_pending_bytes += scheduled_bytes;
		for (auto bytes : task_bytes) {
			pending_task_bytes.push_back(bytes);
		}
	}
	for (idx_t task_idx = 0; task_idx < schedule_count; task_idx++) {
		unique_ptr<AsyncWriteQueueTask> task;
		try {
			task = make_uniq<AsyncWriteQueueTask>(*this, *executor);
		} catch (...) {
			CancelScheduledTasks(schedule_count - task_idx);
			throw;
		}
		try {
			executor->ScheduleTask(std::move(task));
		} catch (...) {
			// The task destructor releases this task's slot. Release the slots for tasks not yet created.
			CancelScheduledTasks(schedule_count - task_idx - 1);
			throw;
		}
	}
}

idx_t AsyncWriteQueue::TakeRequests(deque<PendingRequest> &requests) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(active_tasks > 0);
	D_ASSERT(pending_tasks > 0);
	D_ASSERT(!pending_task_bytes.empty());
	pending_tasks--;
	auto selected_bytes = pending_task_bytes.front();
	pending_task_bytes.pop_front();
	D_ASSERT(scheduled_pending_bytes >= selected_bytes);
	scheduled_pending_bytes -= selected_bytes;
	if (pending_requests.empty()) {
		return 0;
	}

	idx_t request_bytes = 0;
	while (!pending_requests.empty() && request_bytes < selected_bytes) {
		request_bytes += pending_requests.front().Size();
		requests.push_back(std::move(pending_requests.front()));
		pending_requests.pop_front();
	}
	D_ASSERT(request_bytes == selected_bytes);
	D_ASSERT(pending_bytes >= request_bytes);
	pending_bytes -= request_bytes;
	in_flight_bytes += request_bytes;
	return request_bytes;
}

void AsyncWriteQueue::FinishTask(idx_t task_size) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(active_tasks > 0);
	active_tasks--;
	D_ASSERT(in_flight_bytes >= task_size);
	in_flight_bytes -= task_size;
}

void AsyncWriteQueue::CancelScheduledTask() {
	CancelScheduledTasks(1);
}

void AsyncWriteQueue::CancelScheduledTasks(idx_t task_count) {
	if (task_count == 0) {
		return;
	}
	lock_guard<mutex> guard(lock);
	D_ASSERT(active_tasks >= task_count);
	D_ASSERT(pending_tasks >= task_count);
	active_tasks -= task_count;
	pending_tasks -= task_count;
	for (idx_t task_idx = 0; task_idx < task_count; task_idx++) {
		D_ASSERT(!pending_task_bytes.empty());
		auto task_bytes = pending_task_bytes.back();
		pending_task_bytes.pop_back();
		D_ASSERT(scheduled_pending_bytes >= task_bytes);
		scheduled_pending_bytes -= task_bytes;
	}
}

void AsyncWriteQueue::DrainRequests() {
	deque<PendingRequest> requests;
	AsyncWriteQueueTaskGuard guard(*this);
	auto task_size = TakeRequests(requests);
	if (requests.empty()) {
		guard.Finish();
		return;
	}
	guard.SetRequestSize(task_size);
	idx_t request_idx = 0;
	try {
		for (; request_idx < requests.size(); request_idx++) {
			WriteRequest(std::move(requests[request_idx].request));
		}
	} catch (...) {
		auto error_ptr = std::current_exception();
		ErrorData error;
		try {
			std::rethrow_exception(error_ptr);
		} catch (const std::exception &ex) {
			error = ErrorData(ex);
		} catch (...) { // LCOV_EXCL_START
			error = ErrorData("Unknown exception during async write");
		} // LCOV_EXCL_STOP
		request_idx++;
		for (; request_idx < requests.size(); request_idx++) {
			auto &request = requests[request_idx].request;
			auto request_size = requests[request_idx].Size();
			request.payload.reset();
			CompleteRequest(request, request_size, error);
		}
		std::rethrow_exception(error_ptr);
	}
	guard.Finish();
	ScheduleTasksInternal();
}

void AsyncWriteQueue::CompleteRequest(AsyncWriteRequest &request, idx_t size, optional_ptr<const ErrorData> error) {
	if (request.completion) {
		request.completion(request.offset, size, error);
	}
}

void AsyncWriteQueue::WriteRequest(AsyncWriteRequest request) {
	auto request_size = request.Size();
	ErrorData write_error;
	bool has_error = false;
	try {
		WriteBuffer(request.payload->Ptr(), request_size, request.offset);
	} catch (const std::exception &ex) {
		write_error = ErrorData(ex);
		has_error = true;
	} catch (...) { // LCOV_EXCL_START
		write_error = ErrorData("Unknown exception during async write");
		has_error = true;
	} // LCOV_EXCL_STOP

	request.payload.reset();
	if (has_error) {
		CompleteRequest(request, request_size, write_error);
		write_error.Throw();
	}
	CompleteRequest(request, request_size, nullptr);
}

void AsyncWriteQueue::WriteBuffer(data_ptr_t buffer, idx_t size, idx_t offset) {
	if (size == 0) {
		return;
	}
	try {
		target.Write(buffer, size, offset);
	} catch (const IOException &ex) {
		throw IOException("Async write failed for range [offset=%llu, size=%llu]: %s", offset, size, ex.what());
	} catch (const HTTPException &ex) {
		throw HTTPException(Exception::ConstructMessage("Async write failed for range [offset=%llu, size=%llu]: %s",
		                                                offset, size, ex.what()));
	}
}

void AsyncWriteQueue::RethrowTaskError() {
	if (executor && executor->HasError()) {
		executor->ThrowError();
	}
}

void AsyncWriteQueue::WorkOnPendingTask() {
	if (!executor) {
		VerifyOpen();
		return;
	}
	shared_ptr<Task> task;
	if (!executor->GetTask(task)) {
		TaskScheduler::YieldThread();
		return;
	}
	auto result = task->Execute(TaskExecutionMode::PROCESS_ALL);
	D_ASSERT(result != TaskExecutionResult::TASK_BLOCKED);
	task.reset();
}

void AsyncWriteQueue::Flush() {
	bool already_closed;
	{
		lock_guard<mutex> guard(lock);
		already_closed = closed;
	}
	if (already_closed) {
		RethrowTaskError();
		return;
	}
	if (!executor) {
		RethrowTaskError();
		return;
	}

	try {
		ScheduleTasksInternal(true);
		executor->WorkOnTasks();
	} catch (...) {
		try {
			executor->WorkOnTasks();
		} catch (...) {
		}
		throw;
	}
	RethrowTaskError();
}

void AsyncWriteQueue::VerifyDrained() const {
	if (!pending_requests.empty() || pending_bytes != 0 || in_flight_bytes != 0 || active_tasks != 0 ||
	    pending_tasks != 0 || scheduled_pending_bytes != 0 || !pending_task_bytes.empty()) {
		throw InternalException("AsyncWriteQueue still owns submitted writes");
	}
}

void AsyncWriteQueue::CancelPendingRequestsAfterFailure(const ErrorData &error) noexcept {
	deque<PendingRequest> requests;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(active_tasks == 0);
		D_ASSERT(pending_tasks == 0);
		D_ASSERT(in_flight_bytes == 0);
		D_ASSERT(scheduled_pending_bytes == 0);
		D_ASSERT(pending_task_bytes.empty());
		if (active_tasks != 0 || pending_tasks != 0 || in_flight_bytes != 0 || scheduled_pending_bytes != 0 ||
		    !pending_task_bytes.empty()) {
			return;
		}

		requests = std::move(pending_requests);
		pending_bytes = 0;
		closed = true;
	}

	for (auto &pending : requests) {
		auto request_size = pending.Size();
		auto &request = pending.request;
		request.payload.reset();
		try {
			CompleteRequest(request, request_size, error);
		} catch (...) {
		}
	}
}

void AsyncWriteQueue::Close() {
	bool already_closed;
	{
		lock_guard<mutex> guard(lock);
		already_closed = closed;
	}
	if (already_closed) {
		RethrowTaskError();
		return;
	}

	try {
		Flush();
	} catch (...) {
		auto error = std::current_exception();
		auto error_data = ErrorDataFromExceptionPtr(error);
		CancelPendingRequestsAfterFailure(error_data);
		std::rethrow_exception(error);
	}

	lock_guard<mutex> guard(lock);
	VerifyDrained();
	closed = true;
}

void AsyncWriteQueue::VerifyOpen() const {
	if (closed) {
		throw InternalException("Cannot use closed AsyncWriteQueue");
	}
}

ManagedAsyncWriteQueue::PendingWrite::PendingWrite(AsyncWriteRequest request_p)
    : request(std::move(request_p)), size(request.Size()) {
}

idx_t ManagedAsyncWriteQueue::PendingWrite::Size() const {
	return size;
}

ManagedAsyncWriteQueue::ManagedAsyncWriteQueue(ClientContext &client_context_p, AsyncWriteTarget &target_p)
    : client_context(client_context_p), target(target_p) {
	auto &scheduler = TaskScheduler::GetScheduler(client_context);
	auto regular_threads = MaxValue<idx_t>(NumericCast<idx_t>(scheduler.NumberOfThreads()), 1);
	auto async_threads = NumericCast<idx_t>(scheduler.NumberOfAsyncThreads());
	max_active_drain_tasks = MaxValue<idx_t>(async_threads, 1);
	max_pending_bytes = AsyncWriteConfig::MAX_PENDING_BYTES_PER_THREAD * regular_threads;
	min_pending_bytes = MinValue(max_pending_bytes, AsyncWriteConfig::MIN_PENDING_BYTES_PER_THREAD * regular_threads);

	AsyncWriteTarget &async_target = *this;
	write_queue = make_uniq<AsyncWriteQueue>(client_context, async_target);
	if (write_queue->IsAsync() && max_pending_bytes > 0) {
		memory_state = TemporaryMemoryManager::Get(client_context).Register(client_context);
		memory_state->SetMinimumReservation(min_pending_bytes);
		memory_state->SetZero();
	}
}

ManagedAsyncWriteQueue::~ManagedAsyncWriteQueue() {
	lock_guard<mutex> guard(lock);
	auto drained = pending_writes.empty() && pending_bytes == 0 && external_pending_bytes == 0 &&
	               submitted_bytes == 0 && submitted_requests == 0;
	D_ASSERT(closed || drained);
	D_ASSERT(!closed || drained);
}

bool ManagedAsyncWriteQueue::IsAsync() const {
	return write_queue->IsAsync();
}

bool ManagedAsyncWriteQueue::HasError() {
	return write_queue->HasError();
}

void ManagedAsyncWriteQueue::RegisterWrite(unique_ptr<AsyncWritePayload> payload, idx_t offset,
                                           ScheduleMode schedule_mode) {
	if (!payload || payload->Size() == 0) {
		return;
	}
	RegisterWrite(AsyncWriteRequest(std::move(payload), offset), schedule_mode);
}

void ManagedAsyncWriteQueue::RegisterWrite(AsyncWriteRequest request, ScheduleMode schedule_mode) {
	RegisterWriteInternal(std::move(request), 0, schedule_mode);
}

void ManagedAsyncWriteQueue::RegisterAccountedWrite(AsyncWriteRequest request, ScheduleMode schedule_mode) {
	auto request_size = request.Size();
	RegisterWriteInternal(std::move(request), request_size, ScheduleMode::DEFER);
	if (schedule_mode == ScheduleMode::ALLOW) {
		SchedulePendingWrites(SchedulePolicy::FORCE);
	}
}

void ManagedAsyncWriteQueue::AddExternalPendingBytes(idx_t bytes, bool update_memory) {
	if (bytes == 0 || !write_queue->IsAsync()) {
		return;
	}
	{
		lock_guard<mutex> guard(lock);
		VerifyOpen();
		external_pending_bytes += bytes;
	}
	if (update_memory) {
		UpdateMemoryState();
	}
}

void ManagedAsyncWriteQueue::DiscardExternalPendingBytes(idx_t bytes) noexcept {
	if (bytes == 0 || !write_queue->IsAsync()) {
		return;
	}
	lock_guard<mutex> guard(lock);
	D_ASSERT(external_pending_bytes >= bytes);
	if (external_pending_bytes >= bytes) {
		external_pending_bytes -= bytes;
	}
}

void ManagedAsyncWriteQueue::RegisterWriteInternal(AsyncWriteRequest request, idx_t accounted_external_bytes,
                                                   ScheduleMode schedule_mode) {
	if (!request.payload || request.Size() == 0) {
		return;
	}
	RethrowTaskError();

	auto request_size = request.Size();
	if (!write_queue->IsAsync()) {
		VerifyOpen();
		write_queue->Submit(std::move(request));
		return;
	}

	AddCompletionAccounting(request);
	{
		lock_guard<mutex> guard(lock);
		VerifyOpen();
		if (accounted_external_bytes > 0) {
			D_ASSERT(external_pending_bytes >= accounted_external_bytes);
			external_pending_bytes -= accounted_external_bytes;
		}
		pending_writes.emplace_back(std::move(request));
		pending_bytes += request_size;
	}
	UpdateMemoryState();
	if (schedule_mode == ScheduleMode::ALLOW) {
		SchedulePendingWrites();
	}
}

void ManagedAsyncWriteQueue::SchedulePendingWrites(SchedulePolicy policy) {
	if (!write_queue->IsAsync()) {
		VerifyOpen();
		return;
	}
	SchedulePendingWritesInternal(policy);
}

void ManagedAsyncWriteQueue::SchedulePendingWritesInternal(SchedulePolicy policy) {
	if (!write_queue->IsAsync()) {
		return;
	}

	while (true) {
		AsyncWriteRequest request;
		if (!TakePendingWriteRequest(request, policy)) {
			return;
		}
		write_queue->Submit(std::move(request));
	}
}

void ManagedAsyncWriteQueue::UpdateMemoryState(MemoryUpdateMode mode) {
	(void)mode;
	if (!memory_state) {
		return;
	}

	idx_t current_pending_bytes;
	{
		lock_guard<mutex> guard(lock);
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
			// Repeatedly shrinking here would touch shared TMM state on the write-registration hot path.
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

idx_t ManagedAsyncWriteQueue::BackpressureBudget() {
	if (!memory_state) {
		return NumericLimits<idx_t>::Maximum();
	}
	auto reservation = MinValue(memory_state->GetReservation(), max_pending_bytes);
	// If TMM only grants a tiny reservation, do not retain an async backlog. This makes low-memory execution
	// behave close to synchronous writes, but automatically allows overlap again if the reservation grows later.
	if (reservation < AsyncWriteConfig::REMOTE_COALESCE_THRESHOLD) {
		return 0;
	}
	return reservation;
}

idx_t ManagedAsyncWriteQueue::DrainTaskByteBudget() const {
	return drain_task_byte_budget;
}

idx_t ManagedAsyncWriteQueue::TotalPendingBytes() const {
	return pending_bytes + external_pending_bytes + submitted_bytes;
}

idx_t ManagedAsyncWriteQueue::SubmittedByteWindow() const {
	auto task_budget = DrainTaskByteBudget();
	if (task_budget == 0) {
		return NumericLimits<idx_t>::Maximum();
	}
	auto max_tasks = MaxValue<idx_t>(max_active_drain_tasks, 1);
	if (max_tasks > NumericLimits<idx_t>::Maximum() / task_budget) {
		return NumericLimits<idx_t>::Maximum();
	}
	return max_tasks * task_budget;
}

bool ManagedAsyncWriteQueue::TakePendingWriteRequest(AsyncWriteRequest &request, SchedulePolicy policy) {
	lock_guard<mutex> guard(lock);
	if (pending_writes.empty()) {
		return false;
	}
	if (policy == SchedulePolicy::THRESHOLD) {
		if (submitted_bytes >= SubmittedByteWindow()) {
			return false;
		}
		if (submitted_requests > 0 && pending_bytes < DrainTaskByteBudget()) {
			return false;
		}
	}

	auto request_size = pending_writes.front().Size();
	request = std::move(pending_writes.front().request);
	pending_writes.pop_front();
	D_ASSERT(pending_bytes >= request_size);
	pending_bytes -= request_size;
	submitted_bytes += request_size;
	submitted_requests++;
	return true;
}

void ManagedAsyncWriteQueue::AddCompletionAccounting(AsyncWriteRequest &request) {
	auto user_completion = request.completion;
	request.completion = [this, user_completion](idx_t offset, idx_t size, optional_ptr<const ErrorData> error) {
		CompleteSubmittedWrite(offset, size, error);
		if (user_completion) {
			user_completion(offset, size, error);
		}
	};
}

void ManagedAsyncWriteQueue::CompleteSubmittedWrite(idx_t offset, idx_t size, optional_ptr<const ErrorData> error) {
	(void)offset;
	bool refill = false;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(submitted_requests > 0);
		submitted_requests--;
		D_ASSERT(submitted_bytes >= size);
		submitted_bytes -= size;
		refill = !error && !closed && !pending_writes.empty();
	}
	if (refill) {
		SchedulePendingWritesInternal();
	}
}

void ManagedAsyncWriteQueue::ApplyBackpressure() {
	if (!write_queue->IsAsync()) {
		VerifyOpen();
		return;
	}
	RethrowTaskError();
	UpdateMemoryState(MemoryUpdateMode::FORCE);
	SchedulePendingWrites();
	while (true) {
		idx_t current_pending_bytes;
		{
			lock_guard<mutex> guard(lock);
			D_ASSERT(external_pending_bytes == 0);
			current_pending_bytes = TotalPendingBytes();
		}
		if (current_pending_bytes <= BackpressureBudget()) {
			return;
		}
		SchedulePendingWrites(SchedulePolicy::FORCE);
		write_queue->WorkOnPendingTask();
		RethrowTaskError();
	}
}

void ManagedAsyncWriteQueue::WaitAll() {
	bool already_closed;
	{
		lock_guard<mutex> guard(lock);
		already_closed = closed;
	}
	if (already_closed) {
		RethrowTaskError();
		return;
	}
	if (!write_queue->IsAsync()) {
		RethrowTaskError();
		return;
	}

	try {
		UpdateMemoryState(MemoryUpdateMode::FORCE);
		while (true) {
			if (!write_queue->HasError()) {
				SchedulePendingWritesInternal(SchedulePolicy::FORCE);
			}
			write_queue->Flush();
			lock_guard<mutex> guard(lock);
			if (pending_writes.empty() && pending_bytes == 0 && external_pending_bytes == 0 && submitted_bytes == 0 &&
			    submitted_requests == 0) {
				break;
			}
			if (pending_writes.empty() && pending_bytes == 0 && submitted_bytes == 0 && submitted_requests == 0) {
				throw InternalException("ManagedAsyncWriteQueue still tracks external pending writes");
			}
		}
	} catch (...) {
		try {
			write_queue->Flush();
		} catch (...) {
		}
		throw;
	}

	RethrowTaskError();
}

void ManagedAsyncWriteQueue::VerifyDrained() const {
	if (!pending_writes.empty() || pending_bytes != 0 || external_pending_bytes != 0 || submitted_bytes != 0 ||
	    submitted_requests != 0) {
		throw InternalException("ManagedAsyncWriteQueue still owns registered writes");
	}
}

void ManagedAsyncWriteQueue::CancelPendingWritesAfterFailure(const ErrorData &error) noexcept {
	deque<PendingWrite> writes;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(submitted_requests == 0);
		D_ASSERT(submitted_bytes == 0);
		if (submitted_requests != 0 || submitted_bytes != 0) {
			return;
		}

		writes = std::move(pending_writes);
		pending_bytes = 0;
		external_pending_bytes = 0;
		closed = true;
	}

	for (auto &pending : writes) {
		auto request_size = pending.Size();
		auto &request = pending.request;
		request.payload.reset();
		if (request.completion) {
			try {
				request.completion(request.offset, request_size, error);
			} catch (...) {
			}
		}
	}
}

void ManagedAsyncWriteQueue::Close() {
	bool already_closed;
	{
		lock_guard<mutex> guard(lock);
		already_closed = closed;
	}
	if (already_closed) {
		RethrowTaskError();
		return;
	}

	try {
		WaitAll();
		write_queue->Close();
		ReleaseMemoryReservation();
	} catch (...) {
		auto error = std::current_exception();
		try {
			write_queue->Close();
		} catch (...) {
		}
		auto error_data = ErrorDataFromExceptionPtr(error);
		CancelPendingWritesAfterFailure(error_data);
		try {
			ReleaseMemoryReservation();
		} catch (...) {
		}
		std::rethrow_exception(error);
	}

	lock_guard<mutex> guard(lock);
	VerifyDrained();
	closed = true;
}

void ManagedAsyncWriteQueue::ReleaseMemoryReservation() {
	if (!memory_state || memory_request_bytes == 0) {
		return;
	}
	memory_state->SetZero();
	memory_request_bytes = 0;
}

void ManagedAsyncWriteQueue::RethrowTaskError() {
	write_queue->RethrowTaskError();
}

void ManagedAsyncWriteQueue::VerifyOpen() const {
	if (closed) {
		throw InternalException("Cannot use closed ManagedAsyncWriteQueue");
	}
}

void ManagedAsyncWriteQueue::Write(data_ptr_t buffer, idx_t size, idx_t offset) {
	if (size == 0) {
		return;
	}
	target.Write(buffer, size, offset);
}

ManagedAsyncWriteStreamQueue::PendingWrite::PendingWrite(unique_ptr<AsyncWritePayload> payload_p, idx_t offset_p)
    : payload(std::move(payload_p)), offset(offset_p) {
}

idx_t ManagedAsyncWriteStreamQueue::PendingWrite::Size() const {
	return payload->Size();
}

class ManagedAsyncWriteStreamQueue::CoalescedWritePayload : public AsyncWritePayload {
public:
	CoalescedWritePayload(ClientContext &client_context_p, deque<PendingWrite> writes_p, idx_t size_p)
	    : client_context(client_context_p), writes(std::move(writes_p)), size(size_p) {
	}

	data_ptr_t Ptr() override {
		if (writes.size() == 1) {
			return writes.front().payload->Ptr();
		}
		if (!coalesced.get()) {
			coalesced = BufferAllocator::Get(client_context).Allocate(size);
			idx_t offset = 0;
			for (auto &write : writes) {
				auto current_size = write.Size();
				memcpy(coalesced.get() + offset, write.payload->Ptr(), current_size);
				offset += current_size;
				write.payload.reset();
			}
			D_ASSERT(offset == size);
			writes.clear();
		}
		return coalesced.get();
	}

	idx_t Size() const override {
		return size;
	}

private:
	ClientContext &client_context;
	deque<PendingWrite> writes;
	AllocatedData coalesced;
	idx_t size;
};

ManagedAsyncWriteStreamQueue::ManagedAsyncWriteStreamQueue(ClientContext &client_context_p,
                                                           ManagedAsyncWriteStreamTarget &target_p)
    : client_context(client_context_p), target(target_p) {
	auto local_file = target.IsLocalFile();
	coalesce_threshold =
	    local_file ? AsyncWriteConfig::LOCAL_COALESCE_THRESHOLD : AsyncWriteConfig::REMOTE_COALESCE_THRESHOLD;
	first_task_schedule_threshold = local_file ? 1 : coalesce_threshold;
	drain_task_byte_budget = MaxValue(AsyncWriteConfig::DRAIN_TASK_BYTE_BUDGET, coalesce_threshold);
	limit_coalesced_write_size = local_file;

	auto &scheduler = TaskScheduler::GetScheduler(client_context);
	auto async_threads = NumericCast<idx_t>(scheduler.NumberOfAsyncThreads());

	// Positional writes let multiple async requests drain one logical write queue concurrently.
	// Otherwise the stream queue keeps one sequential request active so target ordering remains correct.
	if (target.SupportsPositionalWrites()) {
		drain_mode = DrainMode::POSITIONAL;
		max_active_drain_tasks = MaxValue<idx_t>(async_threads, 1);
	}

	AsyncWriteTarget &async_target = *this;
	write_queue = make_uniq<ManagedAsyncWriteQueue>(client_context, async_target);
}

ManagedAsyncWriteStreamQueue::~ManagedAsyncWriteStreamQueue() {
	lock_guard<mutex> guard(lock);
	auto drained = batch_depth == 0 && pending_writes.empty() && pending_bytes == 0 && submitted_bytes == 0 &&
	               submitted_requests == 0;
	D_ASSERT(closed || drained);
	D_ASSERT(!closed || drained);
}

bool ManagedAsyncWriteStreamQueue::IsAsync() const {
	return write_queue->IsAsync();
}

bool ManagedAsyncWriteStreamQueue::HasError() {
	return write_queue->HasError();
}

void ManagedAsyncWriteStreamQueue::RegisterWrite(unique_ptr<AsyncWritePayload> payload, idx_t offset,
                                                 ScheduleMode schedule_mode) {
	if (!payload || payload->Size() == 0) {
		return;
	}
	RethrowTaskError();

	auto write_size = payload->Size();
	if (!write_queue->IsAsync()) {
		VerifyOpen();
		auto next_offset = ValidateRegistrationOffset(offset, write_size);
		Write(payload->Ptr(), write_size, offset);
		next_registration_offset = next_offset;
		return;
	}

	// Completion-driven refills may schedule pending_writes as soon as they are visible.
	write_queue->AddExternalPendingBytes(write_size, false);
	bool inserted = false;
	bool update_memory = true;
	try {
		{
			lock_guard<mutex> guard(lock);
			VerifyOpen();
			auto next_offset = ValidateRegistrationOffset(offset, write_size);
			pending_writes.emplace_back(std::move(payload), offset);
			pending_bytes += write_size;
			next_registration_offset = next_offset;
			update_memory = batch_depth == 0;
			inserted = true;
		}
	} catch (...) {
		if (!inserted) {
			write_queue->DiscardExternalPendingBytes(write_size);
		}
		throw;
	}
	if (update_memory) {
		write_queue->UpdateMemoryState(ManagedAsyncWriteQueue::MemoryUpdateMode::COARSE);
	}
	if (schedule_mode == ScheduleMode::ALLOW) {
		SchedulePendingWrites();
	}
}

void ManagedAsyncWriteStreamQueue::BeginBatch() {
	if (!write_queue->IsAsync()) {
		VerifyOpen();
		return;
	}
	lock_guard<mutex> guard(lock);
	VerifyOpen();
	batch_depth++;
}

void ManagedAsyncWriteStreamQueue::LeaveBatch() noexcept {
	if (!write_queue->IsAsync()) {
		return;
	}
	lock_guard<mutex> guard(lock);
	if (batch_depth == 0) {
		return;
	}
	batch_depth--;
}

bool ManagedAsyncWriteStreamQueue::HasOpenBatch() {
	if (!write_queue->IsAsync()) {
		return false;
	}
	lock_guard<mutex> guard(lock);
	return batch_depth > 0;
}

void ManagedAsyncWriteStreamQueue::SchedulePendingWrites(SchedulePolicy policy) {
	if (!write_queue->IsAsync()) {
		VerifyOpen();
		return;
	}
	SchedulePendingWritesInternal(policy);
}

void ManagedAsyncWriteStreamQueue::SchedulePendingWritesInternal(SchedulePolicy policy) {
	if (!write_queue->IsAsync()) {
		return;
	}

	while (true) {
		AsyncWriteRequest request;
		if (!TakePendingWriteRequest(request, policy)) {
			return;
		}
		write_queue->RegisterAccountedWrite(std::move(request));
	}
}

idx_t ManagedAsyncWriteStreamQueue::DrainTaskByteBudget() const {
	return MaxValue(drain_task_byte_budget, coalesce_threshold);
}

idx_t ManagedAsyncWriteStreamQueue::TotalPendingBytes() const {
	return pending_bytes + submitted_bytes;
}

idx_t ManagedAsyncWriteStreamQueue::SelectPendingWriteEnd(idx_t start, idx_t &selected_bytes) const {
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

idx_t ManagedAsyncWriteStreamQueue::SelectPhysicalWriteEnd(idx_t start, idx_t &selected_bytes) const {
	D_ASSERT(start < pending_writes.size());
	selected_bytes = 0;
	if (coalesce_threshold == 0) {
		selected_bytes = pending_writes[start].Size();
		return start + 1;
	}

	auto write_size = pending_writes[start].Size();
	if (write_size >= coalesce_threshold) {
		selected_bytes = write_size;
		return start + 1;
	}

	auto write_offset = pending_writes[start].offset;
	if (limit_coalesced_write_size) {
		idx_t end = start;
		while (end < pending_writes.size()) {
			auto next_size = pending_writes[end].Size();
			if (next_size >= coalesce_threshold || selected_bytes + next_size > coalesce_threshold) {
				break;
			}
			VerifyContiguousWrite(pending_writes[end], write_offset + selected_bytes);
			selected_bytes += next_size;
			end++;
		}
		D_ASSERT(end > start);
		return end;
	}

	idx_t selected_budget_bytes;
	auto budget_end = SelectPendingWriteEnd(start, selected_budget_bytes);
	idx_t small_run_size = 0;
	idx_t small_run_end = start;
	while (small_run_end < budget_end && pending_writes[small_run_end].Size() < coalesce_threshold) {
		VerifyContiguousWrite(pending_writes[small_run_end], write_offset + small_run_size);
		small_run_size += pending_writes[small_run_end].Size();
		small_run_end++;
	}

	idx_t end = start;
	while (end < small_run_end) {
		auto next_size = pending_writes[end].Size();
		selected_bytes += next_size;
		end++;
		auto remaining_after_next = small_run_size - selected_bytes;
		if (selected_bytes >= coalesce_threshold &&
		    (remaining_after_next == 0 || remaining_after_next >= coalesce_threshold)) {
			break;
		}
	}
	D_ASSERT(end > start);
	return end;
}

idx_t ManagedAsyncWriteStreamQueue::SubmittedByteWindow() const {
	auto task_budget = DrainTaskByteBudget();
	if (task_budget == 0) {
		return NumericLimits<idx_t>::Maximum();
	}
	auto max_tasks = MaxValue<idx_t>(max_active_drain_tasks, 1);
	if (max_tasks > NumericLimits<idx_t>::Maximum() / task_budget) {
		return NumericLimits<idx_t>::Maximum();
	}
	return max_tasks * task_budget;
}

bool ManagedAsyncWriteStreamQueue::TakePendingWriteRequest(AsyncWriteRequest &request, SchedulePolicy policy) {
	AsyncWriteCompletionCallback completion = [this](idx_t offset, idx_t size, optional_ptr<const ErrorData> error) {
		CompleteSubmittedWrite(offset, size, error);
	};

	lock_guard<mutex> guard(lock);
	if (pending_writes.empty() || batch_depth > 0) {
		return false;
	}
	if (drain_mode == DrainMode::SEQUENTIAL && submitted_requests > 0) {
		return false;
	}
	if (policy == SchedulePolicy::THRESHOLD) {
		if (pending_bytes < first_task_schedule_threshold && submitted_bytes == 0) {
			return false;
		}
		if (submitted_bytes >= SubmittedByteWindow()) {
			return false;
		}
		if (submitted_requests > 0 && pending_bytes < DrainTaskByteBudget()) {
			return false;
		}
	}

	idx_t selected_bytes = 0;
	auto end = SelectPhysicalWriteEnd(0, selected_bytes);
	if (policy == SchedulePolicy::THRESHOLD && selected_bytes < first_task_schedule_threshold) {
		return false;
	}

	auto write_offset = pending_writes.front().offset;
	deque<PendingWrite> writes;
	for (idx_t write_idx = 0; write_idx < end; write_idx++) {
		writes.push_back(std::move(pending_writes.front()));
		pending_writes.pop_front();
	}
	D_ASSERT(pending_bytes >= selected_bytes);
	pending_bytes -= selected_bytes;
	submitted_bytes += selected_bytes;
	submitted_requests++;
	auto payload = CreatePayload(std::move(writes), selected_bytes);
	request = AsyncWriteRequest(std::move(payload), write_offset, std::move(completion));
	return true;
}

unique_ptr<AsyncWritePayload> ManagedAsyncWriteStreamQueue::CreatePayload(deque<PendingWrite> writes, idx_t size) {
	D_ASSERT(!writes.empty());
	auto expected_offset = writes.front().offset;
	for (auto &write : writes) {
		VerifyContiguousWrite(write, expected_offset);
		expected_offset = NextWriteOffset(expected_offset, write.Size());
	}
	if (writes.size() == 1) {
		return std::move(writes.front().payload);
	}
	return make_uniq<CoalescedWritePayload>(client_context, std::move(writes), size);
}

void ManagedAsyncWriteStreamQueue::CompleteSubmittedWrite(idx_t offset, idx_t size,
                                                          optional_ptr<const ErrorData> error) {
	(void)offset;
	bool refill = false;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(submitted_requests > 0);
		submitted_requests--;
		D_ASSERT(submitted_bytes >= size);
		submitted_bytes -= size;
		refill = !error && !closed && batch_depth == 0 && !pending_writes.empty();
	}
	if (refill) {
		SchedulePendingWritesInternal();
	}
}

void ManagedAsyncWriteStreamQueue::ApplyBackpressure() {
	if (!write_queue->IsAsync()) {
		VerifyOpen();
		return;
	}
	RethrowTaskError();
	if (HasOpenBatch()) {
		return;
	}
	write_queue->UpdateMemoryState(ManagedAsyncWriteQueue::MemoryUpdateMode::FORCE);
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
		if (current_pending_bytes <= write_queue->BackpressureBudget()) {
			return;
		}
		SchedulePendingWrites(SchedulePolicy::FORCE);
		write_queue->ApplyBackpressure();
		RethrowTaskError();
	}
}

void ManagedAsyncWriteStreamQueue::WaitAll(BatchDrainMode batch_drain_mode) {
	bool already_closed;
	{
		lock_guard<mutex> guard(lock);
		already_closed = closed;
	}
	if (already_closed) {
		RethrowTaskError();
		return;
	}
	if (!write_queue->IsAsync()) {
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
		write_queue->UpdateMemoryState(ManagedAsyncWriteQueue::MemoryUpdateMode::FORCE);
		while (true) {
			if (!write_queue->HasError()) {
				SchedulePendingWritesInternal(SchedulePolicy::FORCE);
			}
			write_queue->WaitAll();
			lock_guard<mutex> guard(lock);
			if (pending_writes.empty() && pending_bytes == 0 && submitted_bytes == 0 && submitted_requests == 0) {
				break;
			}
		}
	} catch (...) {
		try {
			open_batch_for_drain();
			write_queue->WaitAll();
		} catch (...) {
		}
		restore_batch();
		throw;
	}

	restore_batch();
	RethrowTaskError();
}

void ManagedAsyncWriteStreamQueue::VerifyDrained() const {
	if (batch_depth != 0 || !pending_writes.empty() || pending_bytes != 0 || submitted_bytes != 0 ||
	    submitted_requests != 0) {
		throw InternalException("ManagedAsyncWriteStreamQueue still owns registered writes");
	}
}

void ManagedAsyncWriteStreamQueue::CancelPendingWritesAfterFailure() noexcept {
	idx_t discarded_bytes;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(submitted_requests == 0);
		D_ASSERT(submitted_bytes == 0);
		if (submitted_requests != 0 || submitted_bytes != 0) {
			return;
		}

		discarded_bytes = pending_bytes;
		pending_writes.clear();
		pending_bytes = 0;
		batch_depth = 0;
		closed = true;
	}
	write_queue->DiscardExternalPendingBytes(discarded_bytes);
}

void ManagedAsyncWriteStreamQueue::Close() {
	bool already_closed;
	{
		lock_guard<mutex> guard(lock);
		already_closed = closed;
	}
	if (already_closed) {
		RethrowTaskError();
		return;
	}

	try {
		WaitAll(BatchDrainMode::FORCE_CLOSE_BATCH);
		write_queue->Close();
	} catch (...) {
		auto error = std::current_exception();
		try {
			write_queue->Close();
		} catch (...) {
		}
		CancelPendingWritesAfterFailure();
		try {
			ReleaseMemoryReservation();
		} catch (...) {
		}
		std::rethrow_exception(error);
	}

	lock_guard<mutex> guard(lock);
	VerifyDrained();
	closed = true;
}

void ManagedAsyncWriteStreamQueue::ResetNextOffset(idx_t offset) {
	RethrowTaskError();
	lock_guard<mutex> guard(lock);
	VerifyOpen();
	VerifyDrained();
	next_registration_offset = offset;
}

void ManagedAsyncWriteStreamQueue::ReleaseMemoryReservation() {
	write_queue->ReleaseMemoryReservation();
}

void ManagedAsyncWriteStreamQueue::RethrowTaskError() {
	write_queue->RethrowTaskError();
}

idx_t ManagedAsyncWriteStreamQueue::ValidateRegistrationOffset(idx_t offset, idx_t write_size) const {
	if (offset != next_registration_offset) {
		throw InternalException(
		    "ManagedAsyncWriteStreamQueue only supports contiguous writes: expected offset %llu, got %llu",
		    next_registration_offset, offset);
	}
	return NextWriteOffset(offset, write_size);
}

void ManagedAsyncWriteStreamQueue::VerifyOpen() const {
	if (closed) {
		throw InternalException("Cannot use closed ManagedAsyncWriteStreamQueue");
	}
}

void ManagedAsyncWriteStreamQueue::VerifyContiguousWrite(const PendingWrite &write, idx_t expected_offset) const {
	if (write.offset != expected_offset) {
		throw InternalException(
		    "ManagedAsyncWriteStreamQueue only supports contiguous writes: expected offset %llu, got %llu",
		    expected_offset, write.offset);
	}
}

idx_t ManagedAsyncWriteStreamQueue::NextWriteOffset(idx_t offset, idx_t write_size) const {
	if (write_size > NumericLimits<idx_t>::Maximum() - offset) {
		throw InternalException("ManagedAsyncWriteStreamQueue write offset overflow");
	}
	return offset + write_size;
}

void ManagedAsyncWriteStreamQueue::Write(data_ptr_t buffer, idx_t size, idx_t offset) {
	if (size == 0) {
		return;
	}
	if (drain_mode == DrainMode::POSITIONAL) {
		target.Write(buffer, size, offset);
	} else {
		target.Write(buffer, size);
	}
}

} // namespace duckdb
