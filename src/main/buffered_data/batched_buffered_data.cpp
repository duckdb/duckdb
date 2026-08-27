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

bool BatchedBufferedData::AppendOrBlock(const DataChunk &to_append, idx_t batch, const InterruptState &blocked_sink) {
	// Copy first. The block decision and the byte accounting share one critical
	// section, so racing producers cannot exceed the cap. A block keeps the copy
	// for wake time
	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), to_append.GetTypes());
	to_append.Copy(*chunk, 0);
	const idx_t allocation_size = chunk->GetDataSize();

	shared_ptr<QueryResultNotifier> notifier;
	bool signal_chunk_ready = false;
	{
		annotated_lock_guard<annotated_mutex> lock(glock);
		D_ASSERT(batch >= min_batch);
		max_seen_chunk_bytes = MaxValue<idx_t>(max_seen_chunk_bytes, allocation_size);
		if (ShouldBlockBatch(lock, batch, allocation_size)) {
			// Park holding the finished copy. Restart selection deposits it at wake time
			auto entry = blocked_sinks.emplace(batch, BlockedSink {blocked_sink, allocation_size, std::move(chunk)});
			(void)entry;
			D_ASSERT(entry.second);
			return true;
		}
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
		peak_buffered_bytes = MaxValue<idx_t>(peak_buffered_bytes, read_queue_byte_count + buffer_byte_count);
	}
	if (signal_chunk_ready) {
		// This collector's Sink holds no outer lock, so the buffer signals directly
		SignalChunkAvailable(notifier);
	}
	return false;
}

BatchedBufferedData::BatchedBufferedData(ClientContext &context, bool async)
    : BufferedData(BufferedData::Type::BATCHED, context, async), buffer_byte_count(0), read_queue_byte_count(0),
      min_batch(0) {
	// Both capacities must be able to admit a chunk. The split rounds tiny sizes to zero
	read_queue_capacity = MaxValue<idx_t>((idx_t)(static_cast<double>(total_buffer_size) * 0.6), 1);
	buffer_capacity = MaxValue<idx_t>((idx_t)(static_cast<double>(total_buffer_size) * 0.4), 1);
}

bool BatchedBufferedData::ShouldBlockBatch(annotated_lock_guard<annotated_mutex> &lock, idx_t batch,
                                           idx_t incoming_bytes) {
	if (IsMinimumBatchIndex(lock, batch)) {
		// The read queue admits a chunk that fits, and always one chunk when empty
		return read_queue_byte_count > 0 && read_queue_byte_count + incoming_bytes > ReadQueueCapacity();
	}
	// An oversized non-minimum chunk blocks until its batch becomes the minimum.
	// The advance then moves its sink to the read queue rule above, so it is not lost
	return buffer_byte_count > 0 && buffer_byte_count + incoming_bytes > BufferCapacity();
}

bool BatchedBufferedData::PopMayFreeSinks(annotated_lock_guard<annotated_mutex> &lock) {
	// Pops only shrink the read queue, so only the minimum batch sink can become eligible
	return read_queue_byte_count < ReadQueueCapacity();
}

bool BatchedBufferedDataAsync::ShouldBlockBatch(annotated_lock_guard<annotated_mutex> &lock, idx_t batch,
                                                idx_t incoming_bytes) {
	const idx_t total = read_queue_byte_count + buffer_byte_count;
	if (IsMinimumBatchIndex(lock, batch)) {
		// Only block the minimum batch producer while the consumer has chunks to pop.
		// Every pop lowers the total, so consumption always wakes it again
		return total + incoming_bytes > total_buffer_size && !read_queue.empty();
	}
	// Read-ahead batches may not use up the share reserved for the minimum batch.
	// The reserve covers at least one chunk, so the empty-queue append above stays
	// under the cap. A chunk larger than every chunk seen before can exceed it once,
	// by at most its own size. It then blocks until its batch becomes the minimum.
	const idx_t reserve = MaxValue<idx_t>(MaxValue<idx_t>(total_buffer_size / 8, max_seen_chunk_bytes), 1);
	const idx_t threshold = total_buffer_size > reserve ? total_buffer_size - reserve : 1;
	return total + incoming_bytes > threshold;
}

bool BatchedBufferedDataAsync::PopMayFreeSinks(annotated_lock_guard<annotated_mutex> &lock) {
	// Every pop lowers the shared total that both block conditions check
	return true;
}

bool BatchedBufferedData::BufferIsEmpty() {
	annotated_lock_guard<annotated_mutex> lock(glock);
	return read_queue.empty();
}

bool BatchedBufferedData::IsMinimumBatchIndex(annotated_lock_guard<annotated_mutex> &lock, idx_t batch) {
	return min_batch == batch;
}

void BatchedBufferedData::CollectRestartableSinks(annotated_lock_guard<annotated_mutex> &lock,
                                                  vector<pair<idx_t, BlockedSink>> &to_unblock) {
	D_ASSERT(to_unblock.empty());
	// Reserve first so a failed allocation loses no blocked sink
	to_unblock.reserve(blocked_sinks.size());
	for (auto it = blocked_sinks.begin(); it != blocked_sinks.end();) {
		if (ShouldBlockBatch(lock, it->first, it->second.pending_bytes)) {
			it++;
			continue;
		}
		DepositParked(lock, it->first, it->second);
		to_unblock.emplace_back(it->first, std::move(it->second));
		it = blocked_sinks.erase(it);
	}
}

void BatchedBufferedData::DepositParked(annotated_lock_guard<annotated_mutex> &lock, idx_t batch, BlockedSink &sink) {
	// Deposit the parked copy, so the chunk is visible before the producer wakes.
	// The batch may have become the minimum while the producer was parked.
	// A sink blocked again after a failed restart already deposited and holds none
	if (!sink.pending_chunk) {
		return;
	}
	if (IsMinimumBatchIndex(lock, batch)) {
		read_queue.push_back(std::move(sink.pending_chunk));
		read_queue_byte_count += sink.pending_bytes;
	} else {
		auto &in_progress_batch = buffer[batch];
		in_progress_batch.completed = false;
		buffer_byte_count += sink.pending_bytes;
		in_progress_batch.chunks.push_back(std::move(sink.pending_chunk));
	}
	peak_buffered_bytes = MaxValue<idx_t>(peak_buffered_bytes, read_queue_byte_count + buffer_byte_count);
	sink.pending_bytes = 0;
}

void BatchedBufferedData::InvokeUnblocks(vector<pair<idx_t, BlockedSink>> &to_unblock) {
	// Invoked outside glock. Callback() takes the executor lock
	for (idx_t i = 0; i < to_unblock.size(); i++) {
		try {
			to_unblock[i].second.state.Callback();
		} catch (...) {
			// Block the sinks not yet restarted again so they are not lost
			annotated_lock_guard<annotated_mutex> lock(glock);
			for (idx_t remainder = i + 1; remainder < to_unblock.size(); remainder++) {
				blocked_sinks.emplace(to_unblock[remainder].first, std::move(to_unblock[remainder].second));
			}
			throw;
		}
	}
}

void BatchedBufferedData::UnblockSinks() {
	vector<pair<idx_t, BlockedSink>> to_unblock;
	{
		annotated_lock_guard<annotated_mutex> lock(glock);
		CollectRestartableSinks(lock, to_unblock);
	}
	InvokeUnblocks(to_unblock);
}

void BatchedBufferedData::MoveCompletedBatches(annotated_lock_guard<annotated_mutex> &lock) {
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
	vector<pair<idx_t, BlockedSink>> to_unblock;
	shared_ptr<QueryResultNotifier> notifier;
	bool signal_chunk_ready = false;
	{
		annotated_lock_guard<annotated_mutex> lock(glock);

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
		// The advance frees in-progress bytes AND moves the newly minimum batch's blocked
		// sink to the read queue rule, so the selection must run even when nothing moved
		CollectRestartableSinks(lock, to_unblock);
		// Checked after selection. A wake deposit can also turn the read queue non-empty
		if (was_empty && !read_queue.empty()) {
			signal_chunk_ready = true;
			notifier = result_notifier;
		}
		if (buffer_byte_count == buffered_before && !to_unblock.empty()) {
			// The advance freed no bytes yet woke a sink. Buffered chunks always carry
			// data bytes, so an advance that freed nothing also moved nothing.
			moved_nothing_restarts++;
		}
	}
	// Restart the sinks before signalling. The notifier runs user code, and a throw
	// from it must not lose sinks that are already off the blocked list
	InvokeUnblocks(to_unblock);
	if (signal_chunk_ready) {
		SignalChunkAvailable(notifier);
	}
}

BatchedBufferedDataSync::BatchedBufferedDataSync(ClientContext &context) : BatchedBufferedData(context, false) {
}

BatchedBufferedDataAsync::BatchedBufferedDataAsync(ClientContext &context) : BatchedBufferedData(context, true) {
}

StreamExecutionResult BatchedBufferedDataAsync::ExecuteTaskInternal(StreamQueryResult &result,
                                                                    ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}
	if (!cc->IsActiveResult(context_lock, result)) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}
	const bool interrupted = cc->interrupt_state.load(std::memory_order_relaxed) == ClientInterruptState::INTERRUPTED;
	if (interrupted && !Executor::Get(*cc).HasError()) {
		// A worker error also raises the interrupt flag to stop sibling tasks. Only a flag
		// without an executor error is a real cancel request. This is checked even when
		// chunks are buffered, so a cancel is not detected only once the buffer is drained.
		throw InterruptException();
	}
	if (!interrupted && !BufferIsEmpty()) {
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
	if (!BufferIsEmpty()) {
		// An async consumer can fetch as soon as one chunk exists
		return StreamExecutionResult::CHUNK_READY;
	}
	return MapExecutionResult(execution_result);
}

StreamExecutionResult BatchedBufferedDataSync::ExecuteTaskInternal(StreamQueryResult &result,
                                                                   ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
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

// The condition variable needs a std::unique_lock, which the analysis cannot track
void BatchedBufferedDataAsync::WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
                                            StreamQueryResult &result) DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
	// Wake when the read queue turns non-empty. The bounded wait covers progress that
	// does not append
	annotated_unique_lock<annotated_mutex> lock(glock);
	if (!read_queue.empty()) {
		return;
	}
	chunk_ready_cv.wait_for(lock, std::chrono::milliseconds(Executor::WAIT_TIME));
}

void BatchedBufferedData::CompleteBatch(idx_t batch) {
	annotated_lock_guard<annotated_mutex> lock(glock);
	auto it = buffer.find(batch);
	if (it == buffer.end()) {
		return;
	}

	auto &in_progress_batch = it->second;
	in_progress_batch.completed = true;
}

void BatchedBufferedData::AssertNoBlockedSinks() {
#ifdef D_ASSERT_IS_ENABLED
	annotated_lock_guard<annotated_mutex> lock(glock);
	D_ASSERT(blocked_sinks.empty());
#endif
}

unique_ptr<DataChunk> BatchedBufferedData::Scan() {
	unique_ptr<DataChunk> chunk;
	vector<pair<idx_t, BlockedSink>> to_unblock;
	{
		annotated_lock_guard<annotated_mutex> lock(glock);
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
		// The walk is O(blocked sinks), so only run it when a sink exists and this pop
		// can matter
		if (!blocked_sinks.empty() && PopMayFreeSinks(lock)) {
			CollectRestartableSinks(lock, to_unblock);
		}
	}
	InvokeUnblocks(to_unblock);
	return chunk;
}

} // namespace duckdb
