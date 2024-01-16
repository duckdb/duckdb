#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

SimpleBufferedData::SimpleBufferedData(shared_ptr<ClientContext> context) : BufferedData(std::move(context)) {
	buffered_count = 0;
}

void SimpleBufferedData::BlockSink(BlockedSink blocked_sink) {
	lock_guard<mutex> lock(glock);
	blocked_sinks.push(blocked_sink);
}

bool SimpleBufferedData::BufferIsFull() {
	return buffered_count >= BUFFER_SIZE;
}

void SimpleBufferedData::UnblockSinks(idx_t &estimated_tuples) {
	if (Closed()) {
		return;
	}
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

PendingExecutionResult SimpleBufferedData::ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) {
	if (Closed()) {
		return PendingExecutionResult::EXECUTION_ERROR;
	}
	if (BufferIsFull()) {
		// The buffer isn't empty yet, just return
		return PendingExecutionResult::RESULT_READY;
	}
	idx_t estimated_tuples = 0;
	UnblockSinks(estimated_tuples);
	// Let the executor run until the buffer is no longer empty
	auto res = context->ExecuteTaskInternal(context_lock, result);
	while (!PendingQueryResult::IsFinished(res)) {
		if (buffered_count >= BUFFER_SIZE) {
			break;
		}
		// Check if we need to unblock more sinks to reach the buffer size
		UnblockSinks(estimated_tuples);
		res = context->ExecuteTaskInternal(context_lock, result);
	}
	if (result.HasError()) {
		Close();
	}
	return res;
}

unique_ptr<DataChunk> SimpleBufferedData::Scan() {
	if (Closed()) {
		return nullptr;
	}
	lock_guard<mutex> lock(glock);
	if (buffered_chunks.empty()) {
		Close();
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

void SimpleBufferedData::Append(unique_ptr<DataChunk> chunk) {
	unique_lock<mutex> lock(glock);
	buffered_count += chunk->size();
	// printf("buffered_count: %llu\n", buffered_count.load());
	buffered_chunks.push(std::move(chunk));
}

} // namespace duckdb
