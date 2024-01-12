#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp"

namespace duckdb {

void BatchedBufferedData::AddToBacklog(BlockedSink blocked_sink) {
	lock_guard<mutex> lock(glock);
	if (false) {
		// TODO: figure out if this sink is part of the minimum batch index
		D_ASSERT(!blocked_min);
		blocked_min = make_uniq<BlockedSink>(blocked_sink);
	} else {
		blocked_sinks.push(blocked_sink);
	}
}

BatchedBufferedData::BatchedBufferedData(shared_ptr<ClientContext> context) : BufferedData(std::move(context)) {
}

bool BatchedBufferedData::BufferIsFull() {
	// TODO: figure this out
	return false;
}

void BatchedBufferedData::UnblockSinks() {
	auto &estimated_min = replenish_state.estimated_min_tuples;
	auto &estimated_others = replenish_state.estimated_other_tuples;

	if (blocked_min && current_batch_tuple_count >= CURRENT_BATCH_BUFFER_SIZE) {
		auto &sink = *blocked_min;
		estimated_min += sink.chunk_size;
		sink.state.Callback();
		blocked_min.reset();
	}

	while (!blocked_sinks.empty()) {
		auto &blocked_sink = blocked_sinks.front();
		if (other_batches_tuple_count >= OTHER_BATCHES_BUFFER_SIZE) {
			break;
		}
		estimated_others += blocked_sink.chunk_size;
		blocked_sink.state.Callback();
		blocked_sinks.pop();
	}
}

bool BatchedBufferedData::BuffersAreFull() {
	auto &estimated_min = replenish_state.estimated_min_tuples;
	auto &estimated_others = replenish_state.estimated_other_tuples;

	bool min_filled = current_batch_tuple_count + estimated_min >= CURRENT_BATCH_BUFFER_SIZE;
	bool others_filled = other_batches_tuple_count + estimated_others >= OTHER_BATCHES_BUFFER_SIZE;
	if (min_filled && others_filled) {
		// Maybe stop already if only 'min_filled' is true?
		return true;
	}
	return false;
}

void BatchedBufferedData::ResetReplenishState() {
	replenish_state.estimated_min_tuples = 0;
	replenish_state.estimated_other_tuples = 0;
}

void BatchedBufferedData::ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) {
	if (!context) {
		// Result has already been closed
		return;
	}
	ResetReplenishState();
	if (BuffersAreFull()) {
		return;
	}
	UnblockSinks();
	// Let the executor run until the buffer is no longer empty
	while (!PendingQueryResult::IsFinished(context->ExecuteTaskInternal(context_lock, result))) {
		if (BuffersAreFull()) {
			break;
		}
		// Check if we need to unblock more sinks to reach the buffer size
		UnblockSinks();
	}
}

void BatchedBufferedData::FlushChunks(idx_t minimum_batch_index) {
	lock_guard<mutex> lock(glock);
	queue<idx_t> to_remove;
	for (auto it = in_progress_batches.begin(); it != in_progress_batches.end(); it++) {
		auto batch = it->first;
		if (batch >= minimum_batch_index) {
			// These chunks are not ready to be scanned yet
			break;
		}
		auto &chunks = it->second;
		while (!chunks.empty()) {
			// TODO: keep track of the amount of tuples in 'batches', so we don't overpopulate the buffer
			auto chunk = std::move(chunks.front());
			other_batches_tuple_count -= chunk->size();
			current_batch_tuple_count += chunk->size();
			// Add this chunk to the batches that are scanned from
			batches.push(std::move(chunk));
			chunks.pop();
		}
		to_remove.push(batch);
	}

	// FIXME: this can be more efficient, we can pass in a range of iterators to erase
	// Clean up the emptied queues
	while (!to_remove.empty()) {
		auto batch = to_remove.front();
		to_remove.pop();
		in_progress_batches.erase(batch);
	}
}

unique_ptr<DataChunk> BatchedBufferedData::Scan() {
	lock_guard<mutex> lock(glock);
	if (batches.empty()) {
		context.reset();
		return nullptr;
	}
	auto chunk = std::move(batches.front());
	batches.pop();

	auto count = current_batch_tuple_count.load();
	Printer::Print(StringUtil::Format("Buffer capacity: %d", count));

	if (chunk) {
		current_batch_tuple_count -= chunk->size();
	}
	return chunk;
}

void BatchedBufferedData::Append(unique_ptr<DataChunk> chunk, LocalSinkState &lstate) {
	auto &state = lstate.Cast<BufferedBatchCollectorLocalState>();
	auto batch = lstate.BatchIndex();
	auto &chunks = in_progress_batches[batch];
	// Push the chunk into the queue
	other_batches_tuple_count += chunk->size();
	chunks.push(std::move(chunk));
}

} // namespace duckdb
