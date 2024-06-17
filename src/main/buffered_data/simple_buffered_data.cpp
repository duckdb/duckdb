#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

SimpleBufferedData::SimpleBufferedData(weak_ptr<ClientContext> context)
    : BufferedData(BufferedData::Type::SIMPLE, std::move(context)) {
	buffered_count = 0;
	buffer_size = total_buffer_size;
}

SimpleBufferedData::~SimpleBufferedData() {
}

void SimpleBufferedData::BlockSink(const InterruptState &blocked_sink) {
	lock_guard<mutex> lock(glock);
	blocked_sinks.push(blocked_sink);
}

bool SimpleBufferedData::BufferIsFull() {
	return buffered_count >= BufferSize();
}

void SimpleBufferedData::UnblockSinks() {
	if (Closed()) {
		return;
	}
	if (buffered_count >= BufferSize()) {
		return;
	}
	// Reschedule enough blocked sinks to populate the buffer
	lock_guard<mutex> lock(glock);
	while (!blocked_sinks.empty()) {
		auto &blocked_sink = blocked_sinks.front();
		if (buffered_count >= BufferSize()) {
			// We have unblocked enough sinks already
			break;
		}
		blocked_sink.Callback();
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
	UnblockSinks();
	auto cc = context.lock();
	// Let the executor run until the buffer is no longer empty
	auto res = cc->ExecuteTaskInternal(context_lock, result);
	while (!PendingQueryResult::IsFinished(res)) {
		if (buffered_count >= BufferSize()) {
			break;
		}
		// Check if we need to unblock more sinks to reach the buffer size
		UnblockSinks();
		res = cc->ExecuteTaskInternal(context_lock, result);
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

	if (chunk) {
		auto allocation_size = chunk->GetAllocationSize();
		buffered_count -= allocation_size;
	}
	return chunk;
}

void SimpleBufferedData::Append(const DataChunk &to_append) {
	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), to_append.GetTypes());
	to_append.Copy(*chunk, 0);
	auto allocation_size = chunk->GetAllocationSize();

	unique_lock<mutex> lock(glock);
	buffered_count += allocation_size;
	buffered_chunks.push(std::move(chunk));
}

} // namespace duckdb
