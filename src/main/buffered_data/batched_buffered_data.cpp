#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp"
#include "duckdb/common/stack.hpp"

#include <chrono>

namespace duckdb {

bool BatchedBufferedData::BlockSink(const InterruptState &blocked_sink, idx_t batch) {
	lock_guard<mutex> lock(glock);
	if (!ShouldBlockBatch(lock, batch)) {
		// The tier has room, or a restart raced us and made room: keep producing
		return false;
	}
	auto entry = blocked_sinks.emplace(batch, blocked_sink);
	(void)entry;
	D_ASSERT(entry.second);
	return true;
}

BatchedBufferedData::BatchedBufferedData(ClientContext &context, bool async)
    : BufferedData(BufferedData::Type::BATCHED, context, async), buffer_byte_count(0), read_queue_byte_count(0),
      min_batch(0) {
	// Both tiers must be able to admit a chunk; the split rounds tiny sizes to zero
	read_queue_capacity = MaxValue<idx_t>((idx_t)(static_cast<double>(total_buffer_size) * 0.6), 1);
	buffer_capacity = MaxValue<idx_t>((idx_t)(static_cast<double>(total_buffer_size) * 0.4), 1);
}

bool BatchedBufferedData::ShouldBlockBatch(lock_guard<mutex> &lock, idx_t batch) {
	if (IsMinimumBatchIndex(lock, batch)) {
		// If there is room in the read queue, we want to process the minimum batch
		return read_queue_byte_count >= ReadQueueCapacity();
	}
	return buffer_byte_count >= BufferCapacity();
}

bool BatchedBufferedData::BufferIsEmpty() {
	lock_guard<mutex> lock(glock);
	return read_queue.empty();
}

bool BatchedBufferedData::IsMinimumBatchIndex(lock_guard<mutex> &lock, idx_t batch) {
	return min_batch == batch;
}

void BatchedBufferedData::CollectRestartableSinks(lock_guard<mutex> &lock,
                                                  vector<pair<idx_t, InterruptState>> &to_unblock) {
	D_ASSERT(to_unblock.empty());
	// Reserve first so a failed allocation loses no parked sink
	to_unblock.reserve(blocked_sinks.size());
	for (auto it = blocked_sinks.begin(); it != blocked_sinks.end();) {
		if (ShouldBlockBatch(lock, it->first)) {
			it++;
			continue;
		}
		to_unblock.emplace_back(it->first, it->second);
		it = blocked_sinks.erase(it);
	}
}

void BatchedBufferedData::InvokeUnblocks(const vector<pair<idx_t, InterruptState>> &to_unblock) {
	// Invoked outside glock: Callback() takes the executor lock
	for (idx_t i = 0; i < to_unblock.size(); i++) {
		try {
			to_unblock[i].second.Callback();
		} catch (...) {
			// Re-park the sinks not yet restarted so they are not lost
			lock_guard<mutex> lock(glock);
			for (idx_t remainder = i + 1; remainder < to_unblock.size(); remainder++) {
				blocked_sinks.emplace(to_unblock[remainder].first, to_unblock[remainder].second);
			}
			throw;
		}
	}
}

void BatchedBufferedData::UnblockSinks() {
	vector<pair<idx_t, InterruptState>> to_unblock;
	{
		lock_guard<mutex> lock(glock);
		CollectRestartableSinks(lock, to_unblock);
	}
	InvokeUnblocks(to_unblock);
}

void BatchedBufferedData::MoveCompletedBatches(lock_guard<mutex> &lock) {
	stack<idx_t> to_remove;
	for (auto &it : buffer) {
		auto batch_index = it.first;
		auto &in_progress_batch = it.second;
		if (batch_index > min_batch) {
			break;
		}
		D_ASSERT(in_progress_batch.completed || batch_index == min_batch);
		// min_batch - took longer than others
		// min_batch+1 - completed before min_batch
		// min_batch+2 - completed before min_batch
		// new min_batch
		//
		// To preserve the order, the completed batches have to be processed before we can start scanning the "new
		// min_batch"
		auto &chunks = in_progress_batch.chunks;

		idx_t batch_allocation_size = 0;
		for (auto it = chunks.begin(); it != chunks.end(); it++) {
			auto chunk = std::move(*it);
			auto allocation_size = chunk->GetDataSize();
			batch_allocation_size += allocation_size;
			read_queue.push_back(std::move(chunk));
		}
		// Verification to make sure we're not breaking the order by moving batches before the previous ones have
		// finished
		if (lowest_moved_batch > batch_index) {
			throw InternalException("Lowest moved batch is %d, attempted to move %d afterwards\nAttempted to move %d "
			                        "chunks, of %d bytes in total\nmin_batch is %d",
			                        lowest_moved_batch, batch_index, chunks.size(), batch_allocation_size, min_batch);
		}
		D_ASSERT(lowest_moved_batch <= batch_index);
		lowest_moved_batch = batch_index;

		buffer_byte_count -= batch_allocation_size;
		read_queue_byte_count += batch_allocation_size;
		to_remove.push(batch_index);
	}
	while (!to_remove.empty()) {
		auto batch_index = to_remove.top();
		to_remove.pop();
		buffer.erase(batch_index);
	}
}

void BatchedBufferedData::UpdateMinBatchIndex(idx_t min_batch_index) {
	vector<pair<idx_t, InterruptState>> to_unblock;
	shared_ptr<QueryResultNotifier> notifier;
	bool signal_chunk_ready = false;
	{
		lock_guard<mutex> lock(glock);

		auto old_min_batch = min_batch;
		auto new_min_batch = MaxValue(old_min_batch, min_batch_index);
		if (new_min_batch == min_batch) {
			// No change, early out
			return;
		}
		min_batch = new_min_batch;
		const bool was_empty = read_queue.empty();
		const idx_t buffered_before = buffer_byte_count;
		MoveCompletedBatches(lock);
		if (was_empty && !read_queue.empty()) {
			signal_chunk_ready = true;
			notifier = result_notifier;
		}
		// The advance frees in-progress bytes AND moves the newly minimum batch's parked sink
		// to the read-queue rule, so the selection must run even when nothing moved
		CollectRestartableSinks(lock, to_unblock);
		if (buffer_byte_count == buffered_before && !to_unblock.empty()) {
			// An advance that freed nothing yet woke a sink: the reclassification edge.
			// Buffered chunks always carry data bytes, so freed-none means moved-none.
			moved_nothing_restarts++;
		}
	}
	// Restart the sinks before signalling: the notifier runs user code, and a throw from it
	// must not lose sinks that are already off the parked list
	InvokeUnblocks(to_unblock);
	if (signal_chunk_ready) {
		SignalChunkAvailable(notifier);
	}
}

StreamExecutionResult BatchedBufferedData::ExecuteTaskInternal(StreamQueryResult &result,
                                                               ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}
	if (async) {
		if (!cc->IsActiveResult(context_lock, result)) {
			return StreamExecutionResult::EXECUTION_CANCELLED;
		}
		const bool interrupted =
		    cc->interrupt_state.load(std::memory_order_relaxed) == ClientInterruptState::INTERRUPTED;
		if (interrupted && !Executor::Get(*cc).HasError()) {
			// A worker error also raises the interrupt flag to stop sibling tasks; only a flag
			// without an executor error is a real cancel request. Checked even when chunks are
			// buffered, so a cancel is not detected only once the buffer is drained.
			throw InterruptException();
		}
		if (!interrupted && !BufferIsEmpty()) {
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
		if (!BufferIsEmpty()) {
			// An async consumer can fetch as soon as one chunk exists
			return StreamExecutionResult::CHUNK_READY;
		}
		return MapExecutionResult(execution_result);
	}

	if (!BufferIsEmpty()) {
		// The buffer isn't empty yet, just return
		return StreamExecutionResult::CHUNK_READY;
	}
	// Unblock any pending sinks if the buffer isnt full
	UnblockSinks();
	// Let the executor run until the buffer is no longer empty
	auto execution_result = cc->ExecuteTaskInternal(context_lock, result);
	if (!BufferIsEmpty()) {
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

void BatchedBufferedData::WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
                                       StreamQueryResult &result) {
	// Wake on the read-queue edge; the bounded wait covers progress that does not append
	std::unique_lock<mutex> lock(glock);
	if (!read_queue.empty()) {
		return;
	}
	chunk_ready_cv.wait_for(lock, std::chrono::milliseconds(Executor::WAIT_TIME));
}

void BatchedBufferedData::CompleteBatch(idx_t batch) {
	lock_guard<mutex> lock(glock);
	auto it = buffer.find(batch);
	if (it == buffer.end()) {
		return;
	}

	auto &in_progress_batch = it->second;
	in_progress_batch.completed = true;
}

unique_ptr<DataChunk> BatchedBufferedData::Scan() {
	unique_ptr<DataChunk> chunk;
	vector<pair<idx_t, InterruptState>> to_unblock;
	{
		lock_guard<mutex> lock(glock);
		if (read_queue.empty()) {
			context.reset();
			D_ASSERT(blocked_sinks.empty());
			D_ASSERT(buffer.empty());
			return nullptr;
		}
		chunk = std::move(read_queue.front());
		read_queue.pop_front();
		auto allocation_size = chunk->GetDataSize();
		read_queue_byte_count -= allocation_size;
		// The pop restarts parked producers below the low-water mark
		if (read_queue_byte_count < LowWaterMark(ReadQueueCapacity())) {
			CollectRestartableSinks(lock, to_unblock);
		}
	}
	InvokeUnblocks(to_unblock);
	return chunk;
}

void BatchedBufferedData::Append(const DataChunk &to_append, idx_t batch) {
	// We should never find any chunks with a smaller batch index than the minimum

	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), to_append.GetTypes());
	to_append.Copy(*chunk, 0);
	auto allocation_size = chunk->GetDataSize();

	shared_ptr<QueryResultNotifier> notifier;
	bool signal_chunk_ready = false;
	{
		lock_guard<mutex> lock(glock);
		D_ASSERT(batch >= min_batch);
		auto is_minimum = IsMinimumBatchIndex(lock, batch);
		if (is_minimum) {
			for (auto &it : buffer) {
				auto batch_index = it.first;
				if (batch_index >= min_batch) {
					break;
				}
				// There should not be any batches in the buffer that are lower or equal to the minimum batch index
				throw InternalException("Batches remaining in buffer");
			}
			signal_chunk_ready = read_queue.empty();
			read_queue.push_back(std::move(chunk));
			read_queue_byte_count += allocation_size;
			if (signal_chunk_ready) {
				// The chunk is visible before the wake below, so a woken consumer always finds it
				notifier = result_notifier;
			}
		} else {
			auto &in_progress_batch = buffer[batch];
			auto &chunks = in_progress_batch.chunks;
			in_progress_batch.completed = false;
			buffer_byte_count += allocation_size;
			chunks.push_back(std::move(chunk));
		}
	}
	if (signal_chunk_ready) {
		// This collector's Sink holds no outer lock, so the buffer fires the wake directly
		SignalChunkAvailable(notifier);
	}
}

} // namespace duckdb
