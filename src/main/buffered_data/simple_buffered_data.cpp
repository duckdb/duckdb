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

bool SimpleBufferedData::BufferSaturated() {
	annotated_lock_guard<annotated_mutex> lock(glock);
	return buffered_count >= BufferSize() || !blocked_sinks.empty();
}

idx_t SimpleBufferedData::PeakBufferedBytes() {
	annotated_lock_guard<annotated_mutex> lock(glock);
	return peak_buffered_bytes;
}

bool SimpleBufferedData::HasBufferedChunk() {
	annotated_lock_guard<annotated_mutex> lock(glock);
	return !buffered_chunks.empty();
}

void SimpleBufferedData::CollectRestartableSinks(vector<BlockedSink> &to_unblock) {
	D_ASSERT(to_unblock.empty());
	// Reserve first so a failed allocation loses no blocked sink
	to_unblock.reserve(blocked_sinks.size());
	while (!blocked_sinks.empty()) {
		auto &front = blocked_sinks.front();
		// Sinks restart in FIFO order. Stop at the first chunk that does not fit yet
		if (buffered_count > 0 && buffered_count + front.pending_bytes > BufferSize()) {
			break;
		}
		// Deposit the parked copy, so the chunk is visible before the producer wakes.
		// A sink blocked again after a failed restart already deposited and holds none
		if (front.pending_chunk) {
			buffered_count += front.pending_bytes;
			buffered_chunks.push(std::move(front.pending_chunk));
			peak_buffered_bytes = MaxValue<idx_t>(peak_buffered_bytes, buffered_count);
			front.pending_bytes = 0;
		}
		to_unblock.push_back(std::move(front));
		blocked_sinks.pop();
	}
}

void SimpleBufferedData::InvokeUnblocks(vector<BlockedSink> &to_unblock) {
	// Invoked outside glock. Callback() takes the executor lock
	for (idx_t i = 0; i < to_unblock.size(); i++) {
		try {
			to_unblock[i].state.Callback();
		} catch (...) {
			// Block the sinks not yet restarted again so they are not lost
			annotated_lock_guard<annotated_mutex> lock(glock);
			for (idx_t remainder = i + 1; remainder < to_unblock.size(); remainder++) {
				blocked_sinks.push(std::move(to_unblock[remainder]));
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
	vector<BlockedSink> to_unblock;
	{
		annotated_lock_guard<annotated_mutex> lock(glock);
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
			// A worker error also raises the interrupt flag to stop sibling tasks. Only a flag
			// without an executor error is a real cancel request. This is checked even when
			// chunks are buffered, so a cancel is not detected only once the buffer is drained.
			throw InterruptException();
		}
		if (!interrupted && HasBufferedChunk()) {
			// A chunk is ready and nothing needs the executor lock
			return StreamExecutionResult::CHUNK_READY;
		}
		// Observe the execution state without running tasks. A pushed worker error
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
	if (BufferSaturated()) {
		// A saturated buffer always holds a chunk, because blocking requires a non-empty buffer
		return StreamExecutionResult::CHUNK_READY;
	}
	UnblockSinks();
	// Let the executor run until the buffer is no longer empty
	auto execution_result = cc->ExecuteTaskInternal(context_lock, result);
	if (BufferSaturated()) {
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

void SimpleBufferedData::AssertNoBlockedSinks() {
#ifdef D_ASSERT_IS_ENABLED
	annotated_lock_guard<annotated_mutex> lock(glock);
	D_ASSERT(blocked_sinks.empty());
#endif
}

unique_ptr<DataChunk> SimpleBufferedData::Scan() {
	if (Closed()) {
		return nullptr;
	}

	unique_ptr<DataChunk> chunk;
	vector<BlockedSink> to_unblock;
	{
		annotated_lock_guard<annotated_mutex> lock(glock);
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
		// The pop restarts blocked producers below the low-water mark
		if (buffered_count < LowWaterMark(BufferSize())) {
			CollectRestartableSinks(to_unblock);
		}
	}
	InvokeUnblocks(to_unblock);
	return chunk;
}

bool SimpleBufferedData::AppendOrBlock(const DataChunk &to_append, const InterruptState &blocked_sink) {
	// Copy first. The cap check must use the flat copy's size. The source can be
	// dictionary-compressed and much smaller. A block keeps the copy for wake time.
	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), to_append.GetTypes());
	to_append.Copy(*chunk, 0);
	auto allocation_size = chunk->GetDataSize();

	shared_ptr<QueryResultNotifier> notifier;
	bool was_empty;
	{
		annotated_unique_lock<annotated_mutex> lock(glock);
		// The buffer admits a chunk that fits, and always one chunk when empty
		if (buffered_count > 0 && buffered_count + allocation_size > BufferSize()) {
			// Park holding the finished copy. Restart selection deposits it at wake time
			blocked_sinks.push(BlockedSink {blocked_sink, allocation_size, std::move(chunk)});
			return true;
		}
		was_empty = buffered_chunks.empty();
		buffered_count += allocation_size;
		buffered_chunks.push(std::move(chunk));
		peak_buffered_bytes = MaxValue<idx_t>(peak_buffered_bytes, buffered_count);
		if (was_empty) {
			// The chunk is visible before the wake below, so a woken consumer always finds it
			notifier = result_notifier;
		}
	}
	if (was_empty) {
		SignalChunkAvailable(notifier);
	}
	return false;
}

void SimpleBufferedData::WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
                                      StreamQueryResult &result) {
	// Wake when a chunk is appended. The bounded wait covers progress that does not append
	annotated_unique_lock<annotated_mutex> lock(glock);
	if (!buffered_chunks.empty()) {
		return;
	}
	chunk_ready_cv.wait_for(lock, std::chrono::milliseconds(Executor::WAIT_TIME));
}

} // namespace duckdb
