#include "duckdb/execution/operator/persistent/merge_action_queue.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Chunk Pool
//===--------------------------------------------------------------------===//
//! Chunks are recycled through the pool - a chunk is handed out as a shared_ptr that returns it to the pool once the
//! consumer that scanned it releases it
struct MergeActionQueue::ChunkPool : public enable_shared_from_this<MergeActionQueue::ChunkPool> {
	ChunkPool(Allocator &allocator, vector<LogicalType> types_p, idx_t max_cached_chunks)
	    : allocator(allocator), types(std::move(types_p)), max_cached_chunks(max_cached_chunks) {
	}

	//! Acquire a chunk from the pool and copy `source` into it
	shared_ptr<DataChunk> Copy(DataChunk &source) {
		unique_ptr<DataChunk> result;
		{
			lock_guard<mutex> guard(lock);
			if (!cached_chunks.empty()) {
				result = std::move(cached_chunks.back());
				cached_chunks.pop_back();
			}
		}
		if (!result) {
			result = make_uniq<DataChunk>();
			result->Initialize(allocator, types);
		} else {
			result->Reset();
		}
		source.Copy(*result);

		auto self = shared_from_this();
		return shared_ptr<DataChunk>(result.release(), [self](DataChunk *chunk) {
			unique_ptr<DataChunk> owned(chunk);
			self->Release(std::move(owned));
		});
	}

	void Release(unique_ptr<DataChunk> chunk) {
		lock_guard<mutex> guard(lock);
		if (cached_chunks.size() >= max_cached_chunks) {
			return;
		}
		cached_chunks.push_back(std::move(chunk));
	}

	void Clear() {
		lock_guard<mutex> guard(lock);
		cached_chunks.clear();
	}

	Allocator &allocator;
	vector<LogicalType> types;
	idx_t max_cached_chunks;

	mutex lock;
	vector<unique_ptr<DataChunk>> cached_chunks;
};

//===--------------------------------------------------------------------===//
// Merge Action Queue
//===--------------------------------------------------------------------===//
MergeActionQueue::MergeActionQueue(ClientContext &context, vector<LogicalType> types_p, MergeActionQueueMode mode_p,
                                   idx_t max_buffered_chunks_p)
    : types(std::move(types_p)), mode(mode_p), max_buffered_chunks(max_buffered_chunks_p) {
	if (mode == MergeActionQueueMode::MATERIALIZED) {
		collection = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), types);
		collection->InitializeAppend(append_state);
	} else {
		pool = make_shared_ptr<ChunkPool>(BufferAllocator::Get(context), types, max_buffered_chunks + 1);
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
	{
		lock_guard<mutex> guard(lock);
		if (cancelled || consumer_finished) {
			// nobody is reading from this queue anymore - discard the rows
			return SinkResultType::NEED_MORE_INPUT;
		}
		if (chunks.size() + reserved_slots >= max_buffered_chunks) {
			// the queue is full - block until a consumer has scanned a chunk
			blocked_producers.push_back(interrupt_state);
			return SinkResultType::BLOCKED;
		}
		// reserve a slot so that concurrent producers do not overshoot the buffer size
		reserved_slots++;
	}

	// the merge into re-uses its chunks - copy the data (outside of the lock)
	auto copied_chunk = pool->Copy(chunk);

	vector<InterruptState> consumers;
	{
		lock_guard<mutex> guard(lock);
		reserved_slots--;
		rows_pushed += copied_chunk->size();
		rows_buffered += copied_chunk->size();
		chunks.push_back(std::move(copied_chunk));
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
		rows_buffered = 0;
		consumers = std::move(blocked_consumers);
		blocked_consumers.clear();
		producers = std::move(blocked_producers);
		blocked_producers.clear();
	}
	if (pool) {
		pool->Clear();
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
	{
		lock_guard<mutex> guard(lock);
		rows_buffered -= MinValue<idx_t>(rows_buffered, chunk.size());
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

SourceResultType MergeActionQueue::ScanBounded(DataChunk &chunk, MergeActionQueueScanState &scan_state,
                                               const InterruptState &interrupt_state) {
	shared_ptr<DataChunk> next_chunk;
	vector<InterruptState> producers;
	{
		lock_guard<mutex> guard(lock);
		// the previously scanned chunk has been consumed - hand it back to the pool
		scan_state.current_chunk.reset();
		if (chunks.empty()) {
			if (finished || cancelled) {
				return SourceResultType::FINISHED;
			}
			// no data available (yet) - block until the merge into pushes more data
			blocked_consumers.push_back(interrupt_state);
			return SourceResultType::BLOCKED;
		}
		next_chunk = std::move(chunks.front());
		chunks.pop_front();
		rows_buffered -= next_chunk->size();
		// a slot has freed up - wake up any producer waiting for space
		producers = std::move(blocked_producers);
		blocked_producers.clear();
	}
	CallbackAll(producers);

	// the scan state keeps the chunk alive until the consumer scans the next chunk
	scan_state.current_chunk = std::move(next_chunk);
	chunk.Reference(*scan_state.current_chunk);
	return SourceResultType::HAVE_MORE_OUTPUT;
}

idx_t MergeActionQueue::RowsPushed() const {
	lock_guard<mutex> guard(lock);
	return rows_pushed;
}

idx_t MergeActionQueue::RowsBuffered() const {
	lock_guard<mutex> guard(lock);
	return rows_buffered;
}

} // namespace duckdb
