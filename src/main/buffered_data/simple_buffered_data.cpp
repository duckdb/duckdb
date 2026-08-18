#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"

#include <chrono>

namespace duckdb {

SimpleBufferedData::SimpleBufferedData(ClientContext &context, bool async)
    : BufferedData(BufferedData::Type::SIMPLE, context, async) {
	buffered_count = 0;
	buffer_size = total_buffer_size;
}

SimpleBufferedData::~SimpleBufferedData() {
}

bool SimpleBufferedData::BlockSink(const InterruptState &blocked_sink) {
	lock_guard<mutex> lock(glock);
	if (buffered_count < BufferSize()) {
		// Raced with a pop that may have already run its restart scan: parking now could last forever
		return false;
	}
	blocked_sinks.push(blocked_sink);
	return true;
}

bool SimpleBufferedData::BufferIsFull() {
	return buffered_count >= BufferSize();
}

bool SimpleBufferedData::HasBufferedChunk() {
	lock_guard<mutex> lock(glock);
	return !buffered_chunks.empty();
}

void SimpleBufferedData::CollectRestartableSinks(vector<InterruptState> &to_unblock) {
	// Reserve first so a failed allocation loses no parked sink
	to_unblock.reserve(blocked_sinks.size());
	while (!blocked_sinks.empty() && buffered_count < BufferSize()) {
		to_unblock.push_back(blocked_sinks.front());
		blocked_sinks.pop();
	}
}

void SimpleBufferedData::InvokeUnblocks(vector<InterruptState> &to_unblock) {
	// Invoked outside glock: Callback() takes the executor lock
	for (idx_t i = 0; i < to_unblock.size(); i++) {
		try {
			to_unblock[i].Callback();
		} catch (...) {
			// Re-park the sinks not yet restarted so they are not lost
			lock_guard<mutex> lock(glock);
			for (idx_t remainder = i + 1; remainder < to_unblock.size(); remainder++) {
				blocked_sinks.push(to_unblock[remainder]);
			}
			throw;
		}
	}
}

void SimpleBufferedData::UnblockSinks() {
	auto cc = context.lock();
	if (!cc) {
		return;
	}
	(void)cc;

	if (buffered_count >= BufferSize()) {
		return;
	}
	// Reschedule enough blocked sinks to populate the buffer
	vector<InterruptState> to_unblock;
	{
		lock_guard<mutex> lock(glock);
		CollectRestartableSinks(to_unblock);
	}
	InvokeUnblocks(to_unblock);
}

StreamExecutionResult SimpleBufferedData::ExecuteTaskInternal(StreamQueryResult &result,
                                                              ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}
	if (!cc->IsActiveResult(context_lock, result)) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}
	if (async) {
		const bool interrupted =
		    cc->interrupt_state.load(std::memory_order_relaxed) == ClientInterruptState::INTERRUPTED;
		if (interrupted && !Executor::Get(*cc).HasError()) {
			// A worker error also raises the interrupt flag to stop sibling tasks; only a flag
			// without an executor error is a real cancel request. Checked even when chunks are
			// buffered, so a cancel is not detected only once the buffer is drained.
			throw InterruptException();
		}
		if (!interrupted && HasBufferedChunk()) {
			// Steady state: a chunk is ready and nothing needs the executor lock
			return StreamExecutionResult::CHUNK_READY;
		}
		// Observe execution state (dry run, never runs tasks here); a pushed worker error
		// converts to EXECUTION_ERROR carrying the real message
		auto execution_result = cc->ExecuteTaskInternal(context_lock, result, true);
		if (execution_result == PendingExecutionResult::EXECUTION_ERROR) {
			if (result.HasError()) {
				Close();
			}
			return StreamExecutionResult::EXECUTION_ERROR;
		}
		if (HasBufferedChunk()) {
			// An async consumer can fetch as soon as one chunk exists
			return StreamExecutionResult::CHUNK_READY;
		}
		return MapExecutionResult(execution_result);
	}
	// Check for interrupt even if the buffer is full.
	// Without this check, cancel requests would not be detected until the buffer is drained.
	if (cc->interrupt_state.load(std::memory_order_relaxed) == ClientInterruptState::INTERRUPTED) {
		throw InterruptException();
	}
	if (BufferIsFull()) {
		// The buffer isn't empty yet, just return
		return StreamExecutionResult::CHUNK_READY;
	}
	UnblockSinks();
	// Let the executor run until the buffer is no longer empty
	auto execution_result = cc->ExecuteTaskInternal(context_lock, result);
	if (buffered_count >= BufferSize()) {
		return StreamExecutionResult::CHUNK_READY;
	}
	if (execution_result == PendingExecutionResult::BLOCKED ||
	    execution_result == PendingExecutionResult::RESULT_READY) {
		return StreamExecutionResult::BLOCKED;
	}
	if (result.HasError()) {
		Close();
	}
	return MapExecutionResult(execution_result);
}

unique_ptr<DataChunk> SimpleBufferedData::Scan() {
	if (Closed()) {
		return nullptr;
	}

	unique_ptr<DataChunk> chunk;
	vector<InterruptState> to_unblock;
	{
		lock_guard<mutex> lock(glock);
		if (buffered_chunks.empty()) {
			Close();
			return nullptr;
		}
		chunk = std::move(buffered_chunks.front());
		buffered_chunks.pop();

		if (chunk) {
			auto allocation_size = chunk->GetDataSize();
			buffered_count -= allocation_size;
		}
		// The pop restarts parked producers. The half-full mark batches restarts; the floor
		// of 1 keeps it satisfiable for tiny buffers.
		const idx_t low_water_mark = MaxValue<idx_t>(BufferSize() / 2, 1);
		if (buffered_count < low_water_mark) {
			CollectRestartableSinks(to_unblock);
		}
	}
	InvokeUnblocks(to_unblock);
	return chunk;
}

shared_ptr<QueryResultNotifier> SimpleBufferedData::Append(const DataChunk &to_append) {
	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), to_append.GetTypes());
	to_append.Copy(*chunk, 0);
	auto allocation_size = chunk->GetDataSize();

	shared_ptr<QueryResultNotifier> notifier;
	bool was_empty;
	{
		unique_lock<mutex> lock(glock);
		was_empty = buffered_chunks.empty();
		buffered_count += allocation_size;
		buffered_chunks.push(std::move(chunk));
		if (was_empty && result_notifier) {
			// The chunk is visible before the caller notifies, so a woken consumer always finds it
			notifier = result_notifier;
		}
	}
	if (was_empty) {
		// Wake a blocking fetch parked in WaitForChunk
		chunk_ready_cv.notify_all();
	}
	return notifier;
}

void SimpleBufferedData::WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
                                      StreamQueryResult &result) {
	// Wake on the append edge; the bounded wait covers progress that does not append
	std::unique_lock<mutex> lock(glock);
	if (!buffered_chunks.empty()) {
		return;
	}
	chunk_ready_cv.wait_for(lock, std::chrono::milliseconds(Executor::WAIT_TIME));
}

} // namespace duckdb
