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
    : BufferedData(BufferedData::Type::BATCHED, std::move(context)), other_batches_tuple_count(0),
      current_batch_tuple_count(0), min_batch(0) {
	read_queue_capacity = (idx_t)(total_buffer_size * 0.6);
	in_progress_queue_capacity = (idx_t)(total_buffer_size * 0.4);
}

bool BatchedBufferedData::ShouldBlockBatch(idx_t batch) {
	bool is_minimum = IsMinimumBatchIndex(batch);
	if (is_minimum) {
		return current_batch_tuple_count >= ReadQueueCapacity();
	}
	return other_batches_tuple_count >= InProgressQueueCapacity();
}

bool BatchedBufferedData::BufferIsEmpty() {
	lock_guard<mutex> lock(glock);
	return batches.empty();
}

bool BatchedBufferedData::IsMinimumBatchIndex(idx_t batch) {
	return min_batch == batch;
}

void BatchedBufferedData::UnblockSinks() {
	lock_guard<mutex> lock(glock);
	stack<idx_t> to_remove;
	for (auto it = blocked_sinks.begin(); it != blocked_sinks.end(); it++) {
		auto batch = it->first;
		auto &blocked_sink = it->second;
		const bool is_minimum = IsMinimumBatchIndex(batch);
		if (is_minimum) {
			if (current_batch_tuple_count >= ReadQueueCapacity()) {
				continue;
			}
		} else {
			if (other_batches_tuple_count >= InProgressQueueCapacity()) {
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

void BatchedBufferedData::EnsureBatchExists(idx_t batch) {
	lock_guard<mutex> lock(glock);
	// Make sure a in-progress batch exists for this batch
	// so UpdateMinBatchIndex is aware of it and won't move later batches until this is completed
	auto &buffered_chunks = in_progress_batches[batch];
	(void)buffered_chunks;
}

void BatchedBufferedData::UpdateMinBatchIndex(idx_t min_batch_index) {
	lock_guard<mutex> lock(glock);

	auto old_min_batch = min_batch.load();
	auto new_min_batch = MaxValue(old_min_batch, min_batch_index);
	if (new_min_batch == min_batch) {
		// No change, early out
		return;
	}
	min_batch = new_min_batch;

	stack<idx_t> to_remove;
	for (auto &it : in_progress_batches) {
		auto batch = it.first;
		auto &buffered_chunks = it.second;
		if (batch > min_batch) {
			// This batch is still in progress, can not be fetched from yet
			break;
		}
		if (!buffered_chunks.completed) {
			break;
		}
		D_ASSERT(buffered_chunks.completed || batch == min_batch.load());
		// min_batch - took longer than others
		// min_batch+1 - completed before min_batch
		// min_batch+2 - completed before min_batch
		// new min_batch
		//
		// To preserve the order, the completed batches have to be processed before we can start scanning the "new
		// min_batch"
		auto &chunks = buffered_chunks.chunks;

		idx_t batch_allocation_size = 0;
		for (auto it = chunks.begin(); it != chunks.end(); it++) {
			auto chunk = std::move(*it);
			auto allocation_size = chunk->GetAllocationSize();
			batch_allocation_size += allocation_size;
			batches.push_back(std::move(chunk));
		}
		// Verification to make sure we're not breaking the order by moving batches before the previous ones have
		// finished
		if (lowest_moved_batch >= batch) {
			throw InternalException("Lowest moved batch is %d, attempted to move %d afterwards\nAttempted to move %d "
			                        "chunks, of %d bytes in total\nmin_batch is %d, old min_batch was %d",
			                        lowest_moved_batch, batch, chunks.size(), batch_allocation_size, min_batch.load(),
			                        old_min_batch);
		}
		D_ASSERT(lowest_moved_batch < batch);
		lowest_moved_batch = batch;

		other_batches_tuple_count -= batch_allocation_size;
		current_batch_tuple_count += batch_allocation_size;
		to_remove.push(batch);
	}
	while (!to_remove.empty()) {
		auto batch = to_remove.top();
		to_remove.pop();
		in_progress_batches.erase(batch);
	}
}

PendingExecutionResult BatchedBufferedData::ReplenishBuffer(StreamQueryResult &result,
                                                            ClientContextLock &context_lock) {
	if (Closed()) {
		return PendingExecutionResult::EXECUTION_ERROR;
	}
	// Unblock any pending sinks if the buffer isnt full
	UnblockSinks();
	if (!BufferIsEmpty()) {
		// The buffer isn't empty yet, just return
		return PendingExecutionResult::RESULT_READY;
	}
	// Let the executor run until the buffer is no longer empty
	auto cc = context.lock();
	auto res = cc->ExecuteTaskInternal(context_lock, result);
	while (!PendingQueryResult::IsFinished(res)) {
		if (!BufferIsEmpty()) {
			break;
		}
		// Check if we need to unblock more sinks to reach the buffer size
		UnblockSinks();
		res = cc->ExecuteTaskInternal(context_lock, result);
	}
	return res;
}

void BatchedBufferedData::CompleteBatch(idx_t batch) {
	lock_guard<mutex> lock(glock);
	auto it = in_progress_batches.find(batch);
	if (it == in_progress_batches.end()) {
		return;
	}

	auto &buffered_chunks = it->second;
	buffered_chunks.completed = true;
}

unique_ptr<DataChunk> BatchedBufferedData::Scan() {
	unique_ptr<DataChunk> chunk;
	lock_guard<mutex> lock(glock);
	if (!batches.empty()) {
		chunk = std::move(batches.front());
		batches.pop_front();
		auto allocation_size = chunk->GetAllocationSize();
		current_batch_tuple_count -= allocation_size;
	} else {
		context.reset();
		D_ASSERT(blocked_sinks.empty());
		D_ASSERT(in_progress_batches.empty());
		return nullptr;
	}
	return chunk;
}

void BatchedBufferedData::Append(const DataChunk &to_append, idx_t batch) {
	// We should never find any chunks with a smaller batch index than the minimum
	D_ASSERT(batch >= min_batch);

	bool is_minimum = batch == min_batch;
	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), to_append.GetTypes());
	to_append.Copy(*chunk, 0);
	auto allocation_size = chunk->GetAllocationSize();

	lock_guard<mutex> lock(glock);
	auto &buffered_chunks = in_progress_batches[batch];
	auto &chunks = buffered_chunks.chunks;
	buffered_chunks.completed = false;
	other_batches_tuple_count += allocation_size;
	chunks.push_back(std::move(chunk));
}

} // namespace duckdb
