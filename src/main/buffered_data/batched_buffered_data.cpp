#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/stack.hpp"

namespace duckdb {

BatchedBufferedData::BatchedBufferedData(ClientContext &context, ResultLifetime lifetime)
    : BufferedData(BufferedData::Type::BATCHED, context, lifetime), buffer_byte_count(0), read_queue_byte_count(0),
      min_batch(0) {
}

bool BatchedBufferedData::AppendOrBlock(DataChunk &to_append, idx_t batch, const InterruptState &blocked_sink) {
	// Copied outside the lock: both outcomes need the copy, and parallel producers copy concurrently
	auto copy = CopyForBuffering(to_append);
	const idx_t chunk_data_size = copy->GetDataSize();
	annotated_lock_guard<annotated_mutex> lock(glock);
	D_ASSERT(batch >= min_batch);
	max_seen_chunk_bytes = MaxValue<idx_t>(max_seen_chunk_bytes, chunk_data_size);
	if (ShouldBlockBatch(lock, batch, chunk_data_size)) {
		// Park holding the finished copy. Restart selection deposits it at wake time
		auto entry = blocked_sinks.emplace(batch, BlockedSink {blocked_sink, chunk_data_size, std::move(copy)});
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
		read_queue.push(BufferedChunk {std::move(copy), chunk_data_size});
		read_queue_byte_count += chunk_data_size;
	} else {
		auto &in_progress_batch = buffer[batch];
		in_progress_batch.completed = false;
		in_progress_batch.chunk_refs.push_back(BufferedChunk {std::move(copy), chunk_data_size});
		buffer_byte_count += chunk_data_size;
	}
	peak_buffered_bytes = MaxValue<idx_t>(peak_buffered_bytes, read_queue_byte_count + buffer_byte_count);
	return false;
}

bool BatchedBufferedData::ShouldBlockBatch(annotated_lock_guard<annotated_mutex> &lock, idx_t batch,
                                           idx_t incoming_bytes) {
	const idx_t total = read_queue_byte_count + buffer_byte_count;
	if (IsMinimumBatchIndex(lock, batch)) {
		// Only block the minimum batch producer while the consumer has chunks to pop.
		// Every pop lowers the total, so consumption always wakes it again
		return total + incoming_bytes > total_buffer_size && !read_queue.empty();
	}
	if (total == 0) {
		// An empty pool always admits one chunk, so every producer keeps progressing
		return false;
	}
	// Read-ahead batches leave the minimum batch a reserve: the largest chunk seen, capped at half the
	// budget so one oversized chunk cannot serialize read-ahead for the rest of the query
	const idx_t reserve = MaxValue<idx_t>(MinValue<idx_t>(max_seen_chunk_bytes, total_buffer_size / 2),
	                                      MaxValue<idx_t>(total_buffer_size / 8, 1));
	const idx_t threshold = MaxValue<idx_t>(total_buffer_size - reserve, 1);
	return total + incoming_bytes > threshold;
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
	// Deposit before the wake, so the chunk is visible when the producer resumes; the batch may have
	// become the minimum meanwhile. The null guard keeps a park without a copy out of the queue
	if (!sink.pending_chunk) {
		return;
	}
	if (IsMinimumBatchIndex(lock, batch)) {
		read_queue.push(BufferedChunk {std::move(sink.pending_chunk), sink.pending_bytes});
		read_queue_byte_count += sink.pending_bytes;
	} else {
		auto &in_progress_batch = buffer[batch];
		in_progress_batch.completed = false;
		in_progress_batch.chunk_refs.push_back(BufferedChunk {std::move(sink.pending_chunk), sink.pending_bytes});
		buffer_byte_count += sink.pending_bytes;
	}
	peak_buffered_bytes = MaxValue<idx_t>(peak_buffered_bytes, read_queue_byte_count + buffer_byte_count);
	sink.pending_bytes = 0;
}

void BatchedBufferedData::InvokeUnblocks(const vector<pair<idx_t, BlockedSink>> &to_unblock) {
	// Must be invoked outside glock, Callback() takes the executor lock. A throw here terminates the query, and
	// teardown reclaims the parked tasks.
	for (auto &entry : to_unblock) {
		entry.second.state.Callback();
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
		idx_t batch_data_size = 0;
		for (auto &ref : in_progress_batch.chunk_refs) {
			batch_data_size += ref.data_size;
			read_queue.push(std::move(ref));
		}
		// Verification to make sure we're not breaking the order by moving batches before the previous ones have
		// finished
		if (lowest_moved_batch > batch_index) {
			throw InternalException("Lowest moved batch is %d, attempted to move %d afterwards\nAttempted to move %d "
			                        "chunks, of %d bytes in total\nmin_batch is %d",
			                        lowest_moved_batch, batch_index, in_progress_batch.chunk_refs.size(),
			                        batch_data_size, min_batch);
		}
		D_ASSERT(lowest_moved_batch <= batch_index);
		lowest_moved_batch = batch_index;

		buffer_byte_count -= batch_data_size;
		read_queue_byte_count += batch_data_size;
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
	{
		annotated_lock_guard<annotated_mutex> lock(glock);

		auto old_min_batch = min_batch;
		auto new_min_batch = MaxValue(old_min_batch, min_batch_index);
		if (new_min_batch == min_batch) {
			// No change, early out
			return;
		}
		min_batch = new_min_batch;
		MoveCompletedBatches(lock);
		// The move conserves the byte total, so only the newly minimum batch's sink can
		// have a changed block decision: it now falls under the read queue rule
		auto entry = blocked_sinks.find(min_batch);
		if (entry != blocked_sinks.end() && !ShouldBlockBatch(lock, entry->first, entry->second.pending_bytes)) {
			DepositParked(lock, entry->first, entry->second);
			to_unblock.emplace_back(entry->first, std::move(entry->second));
			blocked_sinks.erase(entry);
		}
	}
	InvokeUnblocks(to_unblock);
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
		auto ref = std::move(read_queue.front());
		read_queue.pop();
		chunk = std::move(ref.chunk);
		read_queue_byte_count -= ref.data_size;
		// The walk is O(blocked sinks), so only run it when a sink exists
		if (!blocked_sinks.empty()) {
			CollectRestartableSinks(lock, to_unblock);
		}
	}
	InvokeUnblocks(to_unblock);
	return chunk;
}

} // namespace duckdb
