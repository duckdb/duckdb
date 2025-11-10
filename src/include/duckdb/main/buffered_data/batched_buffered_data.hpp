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

class InProgressBatch {
public:
	//! The chunks that make up the batch
	deque<unique_ptr<DataChunk>> chunks;
	//! Whether the batch is completed (NextBatch has been called)
	bool completed = false;
};

class BatchedBufferedData : public BufferedData {
public:
	static constexpr const BufferedData::Type TYPE = BufferedData::Type::BATCHED;

public:
	explicit BatchedBufferedData(ClientContext &context);

public:
	void Append(const DataChunk &chunk, idx_t batch);
	void BlockSink(const InterruptState &blocked_sink, idx_t batch);

	bool ShouldBlockBatch(idx_t batch);
	StreamExecutionResult ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) override;
	unique_ptr<DataChunk> Scan() override;
	void UpdateMinBatchIndex(idx_t min_batch_index);
	bool IsMinimumBatchIndex(lock_guard<mutex> &lock, idx_t batch);
	void CompleteBatch(idx_t batch);
	bool BufferIsEmpty();
	void UnblockSinks() override;

	inline idx_t ReadQueueCapacity() const {
		return read_queue_capacity;
	}
	inline idx_t BufferCapacity() const {
		return buffer_capacity;
	}

private:
	void ResetReplenishState();
	void MoveCompletedBatches(lock_guard<mutex> &lock);

private:
	//! The buffer where chunks are written before they are ready to be read.
	map<idx_t, InProgressBatch> buffer;
	idx_t buffer_capacity;
	atomic<idx_t> buffer_byte_count;

	//! The queue containing the chunks that can be read.
	deque<unique_ptr<DataChunk>> read_queue;
	idx_t read_queue_capacity;
	atomic<idx_t> read_queue_byte_count;

	map<idx_t, InterruptState> blocked_sinks;

	idx_t min_batch;
	//! Debug variable to verify that order is preserved correctly.
	idx_t lowest_moved_batch = 0;
};

} // namespace duckdb
