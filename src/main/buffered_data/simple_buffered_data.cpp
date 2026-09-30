#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

SimpleBufferedData::SimpleBufferedData(ClientContext &context, ResultLifetime lifetime)
    : BufferedData(BufferedData::Type::SIMPLE, context, lifetime), buffered_count(0), buffer_size(total_buffer_size) {
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

bool SimpleBufferedData::HasBlockedSink() {
	annotated_lock_guard<annotated_mutex> lock(glock);
	return !blocked_sinks.empty();
}

void SimpleBufferedData::CollectRestartableSinks(annotated_lock_guard<annotated_mutex> &lock,
                                                 vector<BlockedSink> &to_unblock) {
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
		// Parks always carry their copy; the guard keeps a null from corrupting the queue
		if (front.pending_chunk) {
			buffered_count += front.pending_bytes;
			unread_chunks.push(BufferedChunk {std::move(front.pending_chunk), front.pending_bytes});
			peak_buffered_bytes = MaxValue<idx_t>(peak_buffered_bytes, buffered_count);
			front.pending_bytes = 0;
		}
		to_unblock.push_back(std::move(front));
		blocked_sinks.pop();
	}
}

void SimpleBufferedData::InvokeUnblocks(const vector<BlockedSink> &to_unblock) {
	// Invoked outside glock. Callback() takes the executor lock. A throw here
	// terminates the query, and teardown reclaims the parked tasks
	for (auto &blocked : to_unblock) {
		blocked.state.Callback();
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
		CollectRestartableSinks(lock, to_unblock);
	}
	InvokeUnblocks(to_unblock);
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
		if (unread_chunks.empty()) {
			Close();
			return nullptr;
		}
		auto ref = std::move(unread_chunks.front());
		unread_chunks.pop();
		chunk = std::move(ref.chunk);
		buffered_count -= ref.data_size;
		// The pop restarts blocked producers below the low-water mark
		if (buffered_count < LowWaterMark(BufferSize())) {
			CollectRestartableSinks(lock, to_unblock);
		}
	}
	InvokeUnblocks(to_unblock);
	return chunk;
}

bool SimpleBufferedData::AppendOrBlock(DataChunk &to_append, const InterruptState &blocked_sink) {
	// Copied outside the lock: both outcomes need the copy, and parallel producers copy concurrently
	auto copy = CopyForBuffering(to_append);
	const idx_t chunk_data_size = copy->GetDataSize();
	annotated_lock_guard<annotated_mutex> lock(glock);
	// The buffer admits a chunk that fits, and always one chunk when empty
	if (buffered_count > 0 && buffered_count + chunk_data_size > BufferSize()) {
		// Park holding the finished copy. Restart selection deposits it at wake time
		blocked_sinks.push(BlockedSink {blocked_sink, chunk_data_size, std::move(copy)});
		return true;
	}
	unread_chunks.push(BufferedChunk {std::move(copy), chunk_data_size});
	buffered_count += chunk_data_size;
	peak_buffered_bytes = MaxValue<idx_t>(peak_buffered_bytes, buffered_count);
	return false;
}

} // namespace duckdb
