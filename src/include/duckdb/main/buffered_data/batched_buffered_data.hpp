//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/batched_buffered_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {

class StreamQueryResult;

class BufferedChunks {
public:
	deque<unique_ptr<DataChunk>> chunks;
	bool completed = false;
};

class BatchedBufferedData : public BufferedData {
public:
	static constexpr const BufferedData::Type TYPE = BufferedData::Type::BATCHED;

public:
	explicit BatchedBufferedData(weak_ptr<ClientContext> context);

public:
	void Append(const DataChunk &chunk, idx_t batch);
	void BlockSink(const InterruptState &blocked_sink, idx_t batch);

	bool BufferIsEmpty();
	bool ShouldBlockBatch(idx_t batch);
	PendingExecutionResult ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) override;
	unique_ptr<DataChunk> Scan() override;
	void UpdateMinBatchIndex(idx_t min_batch_index);
	bool IsMinimumBatchIndex(idx_t batch);
	void CompleteBatch(idx_t batch);
	void EnsureBatchExists(idx_t batch);

	inline idx_t ReadQueueCapacity() const {
		return read_queue_capacity;
	}
	inline idx_t InProgressQueueCapacity() const {
		return in_progress_queue_capacity;
	}

private:
	void ResetReplenishState();
	void UnblockSinks();

private:
	idx_t read_queue_capacity;
	idx_t in_progress_queue_capacity;

	map<idx_t, InterruptState> blocked_sinks;
	//! The queue of chunks
	deque<unique_ptr<DataChunk>> batches;
	map<idx_t, BufferedChunks> in_progress_batches;

	//! The amount of tuples buffered for the other batches
	atomic<idx_t> other_batches_tuple_count;
	//! The amount of tuples buffered for the current batch
	atomic<idx_t> current_batch_tuple_count;
	atomic<idx_t> min_batch;
	idx_t lowest_moved_batch = 0;
};

} // namespace duckdb
