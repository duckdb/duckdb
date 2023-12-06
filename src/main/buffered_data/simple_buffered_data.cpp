#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/buffered_query_result.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

SimpleBufferedData::SimpleBufferedData(shared_ptr<ClientContext> context) : BufferedData(std::move(context)) {
	buffered_count = 0;
}

void SimpleBufferedData::AddToBacklog(BlockedSink blocked_sink) {
	lock_guard<mutex> lock(glock);
	blocked_sinks.push(blocked_sink);
}

bool SimpleBufferedData::BufferIsFull() const {
	return buffered_count >= BUFFER_SIZE;
}

void SimpleBufferedData::UnblockSinks(idx_t &estimated_tuples) {
	if (buffered_count + estimated_tuples >= BUFFER_SIZE) {
		return;
	}
	// Reschedule enough blocked sinks to populate the buffer
	lock_guard<mutex> lock(glock);
	while (!blocked_sinks.empty()) {
		auto &blocked_sink = blocked_sinks.front();
		if (buffered_count + estimated_tuples >= BUFFER_SIZE) {
			// We have unblocked enough sinks already
			break;
		}
		estimated_tuples += blocked_sink.chunk_size;
		blocked_sink.state.Callback();
		blocked_sinks.pop();
	}
}

void SimpleBufferedData::ReplenishBuffer(BufferedQueryResult &result) {
	if (!context) {
		// Result has already been closed
		return;
	}
	if (BufferIsFull()) {
		// The buffer isn't empty yet, just return
		return;
	}
	idx_t estimated_tuples = 0;
	UnblockSinks(estimated_tuples);
	// Let the executor run until the buffer is no longer empty
	auto context_lock = context->LockContext();
	while (!PendingQueryResult::IsFinished(context->ExecuteTaskInternal(*context_lock, result))) {
		if (buffered_count >= BUFFER_SIZE) {
			break;
		}
		// Check if we need to unblock more sinks to reach the buffer size
		UnblockSinks(estimated_tuples);
	}
}

unique_ptr<DataChunk> SimpleBufferedData::Scan() {
	lock_guard<mutex> lock(glock);
	if (buffered_chunks.empty()) {
		context.reset();
		return nullptr;
	}
	auto chunk = std::move(buffered_chunks.front());
	buffered_chunks.pop();

	// auto count = buffered_count.load();
	// Printer::Print(StringUtil::Format("Buffer capacity: %d", count));

	if (chunk) {
		buffered_count -= chunk->size();
	}
	return chunk;
}

void SimpleBufferedData::Append(unique_ptr<DataChunk> chunk, optional_idx batch) {
	D_ASSERT(!batch.IsValid());
	unique_lock<mutex> lock(glock);
	buffered_count += chunk->size();
	buffered_chunks.push(std::move(chunk));
}

} // namespace duckdb
