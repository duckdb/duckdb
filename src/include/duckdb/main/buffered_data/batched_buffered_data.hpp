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

private:
	//! (roughly) The max amount of tuples we'll keep buffered at a time
	static constexpr idx_t BUFFER_SIZE = 100000;
	static constexpr idx_t CURRENT_BATCH_BUFFER_SIZE = BUFFER_SIZE * 0.6;
	static constexpr idx_t OTHER_BATCHES_BUFFER_SIZE = BUFFER_SIZE * 0.4;

public:
	explicit BatchedBufferedData(weak_ptr<ClientContext> context);

public:
	void Append(unique_ptr<DataChunk> chunk, idx_t batch);
	void BlockSink(const BlockedSink &blocked_sink, idx_t batch);

	bool BufferIsEmpty();
	bool ShouldBlockBatch(idx_t batch);
	PendingExecutionResult ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) override;
	unique_ptr<DataChunk> Scan() override;
	void UpdateMinBatchIndex(idx_t min_batch_index);
	void CompleteBatch(idx_t batch);

private:
	bool IsMinBatch(lock_guard<mutex> &guard, idx_t batch);
	void ResetReplenishState();
	void UnblockSinks();

private:
	map<idx_t, BlockedSink> blocked_sinks;

	//! The queue of chunks
	deque<unique_ptr<DataChunk>> batches;
	map<idx_t, BufferedChunks> in_progress_batches;

	//! The amount of tuples buffered for the other batches
	atomic<idx_t> other_batches_tuple_count;
	//! The amount of tuples buffered for the current batch
	atomic<idx_t> current_batch_tuple_count;

	idx_t min_batch;
};

} // namespace duckdb
