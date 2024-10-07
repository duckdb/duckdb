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
	auto cc = context.lock();
	if (!cc) {
		return;
	}
	(void)cc;

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

StreamExecutionResult SimpleBufferedData::ExecuteTaskInternal(StreamQueryResult &result,
                                                              ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}
	if (!cc->IsActiveResult(context_lock, result)) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}
	if (BufferIsFull()) {
		// The buffer isn't empty yet, just return
		return StreamExecutionResult::CHUNK_READY;
	}
	UnblockSinks();
	// Let the executor run until the buffer is no longer empty
	auto execution_result = cc->ExecuteTaskInternal(context_lock, result);
	if (buffered_count >= BufferSize()) {
		return StreamExecutionResult::CHUNK_READY;
	}
	if (execution_result == PendingExecutionResult::BLOCKED ||
	    execution_result == PendingExecutionResult::RESULT_READY) {
		return StreamExecutionResult::BLOCKED;
	}
	if (result.HasError()) {
		Close();
	}
	switch (execution_result) {
	case PendingExecutionResult::NO_TASKS_AVAILABLE:
	case PendingExecutionResult::RESULT_NOT_READY:
		return StreamExecutionResult::CHUNK_NOT_READY;
	case PendingExecutionResult::EXECUTION_FINISHED:
		return StreamExecutionResult::EXECUTION_FINISHED;
	case PendingExecutionResult::EXECUTION_ERROR:
		return StreamExecutionResult::EXECUTION_ERROR;
	default:
		throw InternalException("No conversion from PendingExecutionResult (%s) -> StreamExecutionResult",
		                        EnumUtil::ToString(execution_result));
	}
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
