#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp"
#include "duckdb/common/stack.hpp"

namespace duckdb {

void BatchedBufferedData::BlockSink(const InterruptState &blocked_sink, idx_t batch) {
	lock_guard<mutex> lock(glock);
	D_ASSERT(!blocked_sinks.count(batch));
	blocked_sinks.emplace(std::make_pair(batch, blocked_sink));
}

BatchedBufferedData::BatchedBufferedData(weak_ptr<ClientContext> context)
    : BufferedData(BufferedData::Type::BATCHED, std::move(context)), buffer_byte_count(0), read_queue_byte_count(0),
      min_batch(0) {
	read_queue_capacity = (idx_t)(total_buffer_size * 0.6);
	buffer_capacity = (idx_t)(total_buffer_size * 0.4);
}

bool BatchedBufferedData::ShouldBlockBatch(idx_t batch) {
	lock_guard<mutex> lock(glock);
	bool is_minimum = IsMinimumBatchIndex(lock, batch);
	if (is_minimum) {
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

void BatchedBufferedData::UnblockSinks() {
	lock_guard<mutex> lock(glock);
	stack<idx_t> to_remove;
	for (auto it = blocked_sinks.begin(); it != blocked_sinks.end(); it++) {
		auto batch = it->first;
		auto &blocked_sink = it->second;
		const bool is_minimum = IsMinimumBatchIndex(lock, batch);
		if (is_minimum) {
			if (read_queue_byte_count >= ReadQueueCapacity()) {
				continue;
			}
		} else {
			if (buffer_byte_count >= BufferCapacity()) {
				continue;
			}
		}
		blocked_sink.Callback();
		to_remove.push(batch);
	}
	while (!to_remove.empty()) {
		auto batch = to_remove.top();
		to_remove.pop();
		blocked_sinks.erase(batch);
	}
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
			auto allocation_size = chunk->GetAllocationSize();
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
	lock_guard<mutex> lock(glock);

	auto old_min_batch = min_batch;
	auto new_min_batch = MaxValue(old_min_batch, min_batch_index);
	if (new_min_batch == min_batch) {
		// No change, early out
		return;
	}
	min_batch = new_min_batch;
	MoveCompletedBatches(lock);
}

bool BatchedBufferedData::ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) {
	if (Closed()) {
		return false;
	}
	// Unblock any pending sinks if the buffer isnt full
	UnblockSinks();
	if (!BufferIsEmpty()) {
		// The buffer isn't empty yet, just return
		return true;
	}
	auto cc = context.lock();
	if (!cc) {
		return false;
	}
	// Let the executor run until the buffer is no longer empty
	PendingExecutionResult execution_result;
	while (!PendingQueryResult::IsExecutionFinished(execution_result = cc->ExecuteTaskInternal(context_lock, result))) {
		if (!BufferIsEmpty()) {
			break;
		}
		if (execution_result == PendingExecutionResult::BLOCKED ||
		    execution_result == PendingExecutionResult::RESULT_READY) {
			// Check if we need to unblock more sinks to reach the buffer size
			UnblockSinks();
			cc->WaitForTask(context_lock, result);
		}
	}
	if (result.HasError()) {
		Close();
	}
	return execution_result != PendingExecutionResult::EXECUTION_ERROR;
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
	lock_guard<mutex> lock(glock);
	if (!read_queue.empty()) {
		chunk = std::move(read_queue.front());
		read_queue.pop_front();
		auto allocation_size = chunk->GetAllocationSize();
		read_queue_byte_count -= allocation_size;
	} else {
		context.reset();
		D_ASSERT(blocked_sinks.empty());
		D_ASSERT(buffer.empty());
		return nullptr;
	}
	return chunk;
}

void BatchedBufferedData::Append(const DataChunk &to_append, idx_t batch) {
	// We should never find any chunks with a smaller batch index than the minimum

	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), to_append.GetTypes());
	to_append.Copy(*chunk, 0);
	auto allocation_size = chunk->GetAllocationSize();

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
		read_queue.push_back(std::move(chunk));
		read_queue_byte_count += allocation_size;
	} else {
		auto &in_progress_batch = buffer[batch];
		auto &chunks = in_progress_batch.chunks;
		in_progress_batch.completed = false;
		buffer_byte_count += allocation_size;
		chunks.push_back(std::move(chunk));
	}
}

} // namespace duckdb
