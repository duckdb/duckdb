#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp"
#include "duckdb/common/stack.hpp"

namespace duckdb {

void BatchedBufferedData::BlockSink(BlockedSink blocked_sink, idx_t batch) {
	lock_guard<mutex> lock(glock);
	D_ASSERT(!blocked_sinks.count(batch));
	blocked_sinks.emplace(std::make_pair(batch, blocked_sink));
}

BatchedBufferedData::BatchedBufferedData(shared_ptr<ClientContext> context)
    : BufferedData(std::move(context)), other_batches_tuple_count(0), current_batch_tuple_count(0), min_batch(0) {
}

bool BatchedBufferedData::ShouldBlockBatch(idx_t batch) {
	lock_guard<mutex> lock(glock);
	// If a batch index is specified we need to check only one of the two tuple_counts
	bool is_minimum = IsMinBatch(lock, batch);
	if (is_minimum) {
		return current_batch_tuple_count >= CURRENT_BATCH_BUFFER_SIZE;
	}
	return other_batches_tuple_count >= OTHER_BATCHES_BUFFER_SIZE;
}

bool BatchedBufferedData::BufferIsFull() {
	lock_guard<mutex> lock(glock);
	if (min_batch == 0) {
		// This is highly unlikely to happen
		// But it's possible that a Sink has to be unblocked before a min batch index is assigned
		// After a Sink has been blocked once, the second time around it'll reach this method

		// TODO: maybe check the `other_batches_tuple_count` to make sure we're not flooding it
		return false;
	}

	bool min_filled = current_batch_tuple_count >= CURRENT_BATCH_BUFFER_SIZE;
	bool others_filled = other_batches_tuple_count >= OTHER_BATCHES_BUFFER_SIZE;
	if (batches.empty()) {
		// If there is no batch to scan, we can't break out of the loop
		// Once the execution is properly finished, we'll break out through a different condition
		return false;
	}
	return min_filled || others_filled;
}

bool BatchedBufferedData::IsMinBatch(lock_guard<mutex> &guard, idx_t batch) {
	if (min_batch == 0) {
		return false;
	}
	return min_batch == batch;
}

void BatchedBufferedData::UnblockSinks() {
	lock_guard<mutex> lock(glock);
	stack<idx_t> to_remove;
	for (auto it = blocked_sinks.begin(); it != blocked_sinks.end(); it++) {
		auto batch = it->first;
		auto &blocked_sink = it->second;
		const bool is_minimum = IsMinBatch(lock, batch);
		if (is_minimum) {
			if (current_batch_tuple_count >= CURRENT_BATCH_BUFFER_SIZE) {
				continue;
			}
		} else {
			if (other_batches_tuple_count >= OTHER_BATCHES_BUFFER_SIZE) {
				continue;
			}
		}
		blocked_sink.state.Callback();
		to_remove.push(batch);
	}
	while (!to_remove.empty()) {
		auto batch = to_remove.top();
		to_remove.pop();
		blocked_sinks.erase(batch);
	}
}

void BatchedBufferedData::UpdateMinBatchIndex(idx_t min_batch_index) {
	if (min_batch_index == 0) {
		// Not a valid minimum batch index
		return;
	}
	lock_guard<mutex> lock(glock);
	if (min_batch_index <= min_batch) {
		// This batch index is outdated or already processed
		// Just verify that there are no "in progress" chunks left for this batch index
		D_ASSERT(!in_progress_batches.count(min_batch_index));
		return;
	}
	min_batch = min_batch_index;
	auto existing_chunks_it = in_progress_batches.find(min_batch_index);
	if (existing_chunks_it == in_progress_batches.end()) {
		// No chunks have been created for this batch index yet
		return;
	}
	// We have already materialized chunks, have to move them to `batches` so they be scanned
	auto &existing_chunks = existing_chunks_it->second;
	idx_t tuple_count = 0;
	for (auto it = existing_chunks.begin(); it != existing_chunks.end(); it++) {
		auto chunk = std::move(*it);
		tuple_count += chunk->size();
		batches.push_back(std::move(chunk));
	}
	other_batches_tuple_count -= tuple_count;
	current_batch_tuple_count += tuple_count;
}

void BatchedBufferedData::ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) {
	if (!context) {
		// Result has already been closed
		return;
	}
	if (BufferIsFull()) {
		return;
	}
	UnblockSinks();
	// Let the executor run until the buffer is no longer empty
	while (!PendingQueryResult::IsFinished(context->ExecuteTaskInternal(context_lock, result))) {
		if (BufferIsFull()) {
			break;
		}
		// Check if we need to unblock more sinks to reach the buffer size
		UnblockSinks();
	}
}

unique_ptr<DataChunk> BatchedBufferedData::Scan() {
	lock_guard<mutex> lock(glock);
	unique_ptr<DataChunk> chunk;
	if (!batches.empty()) {
		chunk = std::move(batches.front());
		batches.pop_front();
	}

	if (!chunk) {
		context.reset();
		D_ASSERT(blocked_sinks.empty());
		return nullptr;
	}

	current_batch_tuple_count -= chunk->size();
	return chunk;
}

void BatchedBufferedData::Append(unique_ptr<DataChunk> chunk, idx_t batch) {
	lock_guard<mutex> lock(glock);
	if (batch == min_batch) {
		current_batch_tuple_count += chunk->size();
		batches.push_back(std::move(chunk));
	} else {
		auto &chunks = in_progress_batches[batch];
		other_batches_tuple_count += chunk->size();
		chunks.push_back(std::move(chunk));
	}
}

} // namespace duckdb
