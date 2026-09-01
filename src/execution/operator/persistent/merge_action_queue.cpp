#include "duckdb/execution/operator/persistent/merge_action_queue.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

MergeActionQueue::MergeActionQueue(ClientContext &context, vector<LogicalType> types_p, MergeActionQueueMode mode_p,
                                   idx_t max_buffered_chunks_p)
    : allocator(BufferAllocator::Get(context)), types(std::move(types_p)), mode(mode_p),
      max_buffered_chunks(max_buffered_chunks_p), rows_pushed(0), rows_buffered(0) {
	if (mode == MergeActionQueueMode::MATERIALIZED) {
		collection = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), types);
		collection->InitializeAppend(append_state);
	}
}

MergeActionQueue::~MergeActionQueue() {
}

void MergeActionQueue::CallbackAll(const vector<InterruptState> &states) {
	for (auto &state : states) {
		state.Callback();
	}
}

SinkResultType MergeActionQueue::Push(DataChunk &chunk, const InterruptState &interrupt_state) {
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (mode == MergeActionQueueMode::MATERIALIZED) {
		return PushMaterialized(chunk);
	}
	return PushBounded(chunk, interrupt_state);
}

SinkResultType MergeActionQueue::PushMaterialized(DataChunk &chunk) {
	lock_guard<mutex> guard(lock);
	if (cancelled || consumer_finished) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	collection->Append(append_state, chunk);
	rows_pushed += chunk.size();
	rows_buffered += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType MergeActionQueue::PushBounded(DataChunk &chunk, const InterruptState &interrupt_state) {
	// the queue is bounded by the number of chunk buffers that exist - a buffer is either free, being filled by a
	// producer, waiting to be scanned, or being scanned by a consumer
	unique_ptr<DataChunk> buffer;
	{
		lock_guard<mutex> guard(lock);
		if (cancelled || consumer_finished) {
			// nobody is reading from this queue anymore - discard the rows
			return SinkResultType::NEED_MORE_INPUT;
		}
		if (!free_chunks.empty()) {
			buffer = std::move(free_chunks.back());
			free_chunks.pop_back();
			buffer->Reset();
		} else if (buffer_count >= max_buffered_chunks) {
			// all buffers are in use - block until a consumer releases one
			blocked_producers.push_back(interrupt_state);
			return SinkResultType::BLOCKED;
		} else {
			// claim a new buffer - it is allocated below
			buffer_count++;
		}
	}
	if (!buffer) {
		buffer = make_uniq<DataChunk>();
		buffer->Initialize(allocator, types);
	}
	// the merge into re-uses its chunks - copy the data (outside of the lock)
	chunk.Copy(*buffer);

	vector<InterruptState> consumers;
	{
		lock_guard<mutex> guard(lock);
		if (cancelled || consumer_finished) {
			free_chunks.push_back(std::move(buffer));
			return SinkResultType::NEED_MORE_INPUT;
		}
		rows_pushed += buffer->size();
		rows_buffered += buffer->size();
		chunks.push_back(std::move(buffer));
		consumers = std::move(blocked_consumers);
		blocked_consumers.clear();
	}
	CallbackAll(consumers);
	return SinkResultType::NEED_MORE_INPUT;
}

void MergeActionQueue::Finish() {
	vector<InterruptState> consumers;
	{
		lock_guard<mutex> guard(lock);
		finished = true;
		consumers = std::move(blocked_consumers);
		blocked_consumers.clear();
	}
	CallbackAll(consumers);
}

void MergeActionQueue::ConsumerFinished() {
	vector<InterruptState> producers;
	{
		lock_guard<mutex> guard(lock);
		consumer_finished = true;
		producers = std::move(blocked_producers);
		blocked_producers.clear();
	}
	// the buffered chunks are not cleared here - another consumer may still be scanning them
	CallbackAll(producers);
}

void MergeActionQueue::Cancel() {
	vector<InterruptState> consumers;
	vector<InterruptState> producers;
	{
		lock_guard<mutex> guard(lock);
		cancelled = true;
		chunks.clear();
		free_chunks.clear();
		consumers = std::move(blocked_consumers);
		blocked_consumers.clear();
		producers = std::move(blocked_producers);
		blocked_producers.clear();
	}
	CallbackAll(consumers);
	CallbackAll(producers);
}

SourceResultType MergeActionQueue::Scan(DataChunk &chunk, MergeActionQueueScanState &scan_state,
                                        const InterruptState &interrupt_state) {
	if (mode == MergeActionQueueMode::MATERIALIZED) {
		return ScanMaterialized(chunk, scan_state, interrupt_state);
	}
	return ScanBounded(chunk, scan_state, interrupt_state);
}

SourceResultType MergeActionQueue::ScanMaterialized(DataChunk &chunk, MergeActionQueueScanState &scan_state,
                                                    const InterruptState &interrupt_state) {
	{
		lock_guard<mutex> guard(lock);
		if (cancelled) {
			return SourceResultType::FINISHED;
		}
		if (!finished) {
			// the rows are only handed out once the merge into has pushed all of them
			blocked_consumers.push_back(interrupt_state);
			return SourceResultType::BLOCKED;
		}
		if (!scan_state.scan_initialized) {
			collection->InitializeScan(scan_state.scan_state);
			scan_state.scan_initialized = true;
		}
	}
	// the data is immutable once the producers are done - scan it without holding the lock
	if (!collection->Scan(scan_state.scan_state, chunk)) {
		return SourceResultType::FINISHED;
	}
	rows_buffered -= chunk.size();
	return SourceResultType::HAVE_MORE_OUTPUT;
}

SourceResultType MergeActionQueue::ScanBounded(DataChunk &chunk, MergeActionQueueScanState &scan_state,
                                               const InterruptState &interrupt_state) {
	unique_ptr<DataChunk> next_chunk;
	vector<InterruptState> producers;
	SourceResultType result;
	{
		lock_guard<mutex> guard(lock);
		if (scan_state.current_chunk) {
			// the previously scanned chunk has been consumed - the buffer can be re-used by a producer
			free_chunks.push_back(std::move(scan_state.current_chunk));
			producers = std::move(blocked_producers);
			blocked_producers.clear();
		}
		if (cancelled) {
			result = SourceResultType::FINISHED;
		} else if (!chunks.empty()) {
			next_chunk = std::move(chunks.front());
			chunks.pop_front();
			rows_buffered -= next_chunk->size();
			result = SourceResultType::HAVE_MORE_OUTPUT;
		} else if (finished) {
			result = SourceResultType::FINISHED;
		} else {
			// no data available (yet) - block until the merge into pushes more data
			blocked_consumers.push_back(interrupt_state);
			result = SourceResultType::BLOCKED;
		}
	}
	// a buffer has freed up - wake up any producer that was waiting for space
	CallbackAll(producers);
	if (result == SourceResultType::HAVE_MORE_OUTPUT) {
		// the scan state keeps the chunk alive until the consumer scans the next chunk
		scan_state.current_chunk = std::move(next_chunk);
		chunk.Reference(*scan_state.current_chunk);
	}
	return result;
}

} // namespace duckdb
