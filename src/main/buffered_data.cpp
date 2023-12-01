#include "duckdb/main/buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/buffered_query_result.hpp"

namespace duckdb {

void BufferedData::AddToBacklog(InterruptState state) {
	lock_guard<mutex> lock(glock);
	blocked_sinks.push(state);
}

bool BufferedData::BufferIsFull() const {
	return buffered_chunks_count >= BUFFER_SIZE;
}

void BufferedData::ReplenishBuffer(BufferedQueryResult &result) {
	if (!context) {
		// Result has already been closed
		return;
	}
	unique_lock<mutex> lock(glock);
	if (BufferIsFull()) {
		// The buffer isn't empty yet, just return
		return;
	}
	// Reschedule all the blocked sinks
	while (!blocked_sinks.empty()) {
		auto &state = blocked_sinks.front();
		state.Callback();
		blocked_sinks.pop();
	}
	// We have to release the lock so the ResultCollector can fill the buffer
	lock.unlock();
	// Let the executor run until the buffer is no longer empty
	auto context_lock = context->LockContext();
	while (!PendingQueryResult::IsFinished(context->ExecuteTaskInternal(*context_lock, result))) {
		if (buffered_chunks_count >= BUFFER_SIZE) {
			break;
		}
	}
}

unique_ptr<DataChunk> BufferedData::Fetch(BufferedQueryResult &result) {
	ReplenishBuffer(result);

	unique_lock<mutex> lock(glock);
	if (!context || buffered_chunks.empty()) {
		context.reset();
		return nullptr;
	}

	// Take a chunk from the queue
	auto chunk = std::move(buffered_chunks.front());
	buffered_chunks.pop();
	auto count = buffered_chunks_count.load();
	Printer::Print(StringUtil::Format("Buffer capacity: %d", count));
	buffered_chunks_count--;
	return chunk;
}

void BufferedData::Populate(unique_ptr<DataChunk> chunk) {
	unique_lock<mutex> lock(glock);
	buffered_chunks.push(std::move(chunk));
	buffered_chunks_count++;
}

} // namespace duckdb
