#include "duckdb/main/buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/buffered_query_result.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

void BufferedData::AddToBacklog(BlockedSink blocked_sink) {
	lock_guard<mutex> lock(glock);
	blocked_sinks.push(blocked_sink);
}

bool BufferedData::BufferIsFull() const {
	return buffered_count >= BUFFER_SIZE;
}

void BufferedData::UnblockSinks(idx_t &estimated_tuples) {
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

void BufferedData::ReplenishBuffer(BufferedQueryResult &result) {
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

unique_ptr<DataChunk> BufferedData::Fetch(BufferedQueryResult &result) {
	ReplenishBuffer(result);

	unique_lock<mutex> lock(glock);
	if (!context || buffered_chunks.empty()) {
		context.reset();
		return nullptr;
	}

	unique_ptr<DataChunk> chunk;
	idx_t remaining = STANDARD_VECTOR_SIZE;
	while (remaining > 0) {
		if (!scan_state.chunk) {
			scan_state.chunk = Scan();
			if (!scan_state.chunk || scan_state.chunk->size() == 0) {
				// Nothing left to scan
				scan_state.chunk.reset();
				break;
			}
			scan_state.offset = 0;
		}
		auto chunk_size = scan_state.chunk->size();
		D_ASSERT(chunk_size > scan_state.offset);
		// How many tuples are still left in our scanned chunk
		auto left_in_chunk = chunk_size - scan_state.offset;
		// How many we can append to the chunk we're currently creating
		auto to_append = MinValue(left_in_chunk, remaining);

		// First make sure we have a result chunk
		if (!chunk) {
			if (!scan_state.offset) {
				// No tuples have been scanned from this yet, just take it
				chunk = std::move(scan_state.chunk);
				scan_state.chunk.reset();
			} else {
				chunk = make_uniq<DataChunk>();
				chunk->Initialize(Allocator::DefaultAllocator(), scan_state.chunk->GetTypes(), STANDARD_VECTOR_SIZE);
				chunk->SetCardinality(0);
			}
		}

		if (scan_state.chunk) {
			// We have not moved the chunk, need to scan from it
			for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
				D_ASSERT(scan_state.chunk->data[i].GetVectorType() == VectorType::FLAT_VECTOR);
				VectorOperations::CopyPartial(scan_state.chunk->data[i], chunk->data[i], scan_state.chunk->size(),
				                              scan_state.offset, to_append, chunk->size());
			}
			chunk->SetCardinality(chunk->size() + to_append);
		}

		// Reset the scan state if necessary
		scan_state.offset += to_append;
		if (scan_state.offset >= chunk_size) {
			scan_state.offset = 0;
			scan_state.chunk.reset();
		}
		remaining -= to_append;
	}
	if (chunk) {
		chunk->SetCardinality(STANDARD_VECTOR_SIZE - remaining);
	}
	return chunk;
}

unique_ptr<DataChunk> BufferedData::Scan() {
	auto chunk = std::move(buffered_chunks.front());
	buffered_chunks.pop();

	// auto count = buffered_count.load();
	// Printer::Print(StringUtil::Format("Buffer capacity: %d", count));

	if (chunk) {
		buffered_count -= chunk->size();
	}
	return chunk;
}

void BufferedData::Append(unique_ptr<DataChunk> chunk) {
	unique_lock<mutex> lock(glock);
	buffered_count += chunk->size();
	buffered_chunks.push(std::move(chunk));
}

} // namespace duckdb
