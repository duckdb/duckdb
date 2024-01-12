//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/batched_buffered_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"

namespace duckdb {

class StreamQueryResult;

// FIXME: should this be a map so we can make a "more accurate" guess as to how many tuples are expected when the min
// batch index changes? Currently we reset the 'estimated_min_tuples' to 0 if the min batch index changes
struct ReplenishBufferState {
public:
	ReplenishBufferState() {
	}

public:
	void Reset();

public:
	idx_t estimated_min_tuples = 0;
	idx_t estimated_other_tuples = 0;
};

class BatchedBufferedData : public BufferedData {
private:
	//! (roughly) The max amount of tuples we'll keep buffered at a time
	static constexpr idx_t BUFFER_SIZE = 100000;
	static constexpr idx_t CURRENT_BATCH_BUFFER_SIZE = BUFFER_SIZE * 0.6;
	static constexpr idx_t OTHER_BATCHES_BUFFER_SIZE = BUFFER_SIZE * 0.4;

public:
	BatchedBufferedData(shared_ptr<ClientContext> context);

public:
	void Append(unique_ptr<DataChunk> chunk, LocalSinkState &state) override;
	void AddToBacklog(BlockedSink blocked_sink) override;
	bool BufferIsFull() override;
	void ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) override;
	unique_ptr<DataChunk> Scan() override;
	void FlushChunks(idx_t minimum_batch_index);

private:
	void ResetReplenishState();
	void UnblockSinks();
	bool BuffersAreFull();

private:
	unique_ptr<BlockedSink> blocked_min;
	queue<BlockedSink> blocked_sinks;
	//! The queue of chunks
	queue<unique_ptr<DataChunk>> batches;
	map<idx_t, queue<unique_ptr<DataChunk>>> in_progress_batches;

	//! The amount of tuples buffered for the other batches
	atomic<idx_t> other_batches_tuple_count;
	//! The amount of tuples buffered for the current batch
	atomic<idx_t> current_batch_tuple_count;

	//! The estimated tuples we're expecting in ReplenishBuffer
	//! This is an optimization to reduce the amount of times a Sink gets unblocked only to block right away
	ReplenishBufferState replenish_state;
};

} // namespace duckdb
