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
#include "duckdb/common/pair.hpp"
#include "duckdb/common/vector.hpp"

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
	explicit BatchedBufferedData(ClientContext &context, bool async = false);

public:
	void Append(const DataChunk &chunk, idx_t batch);
	//! Park the sink if its tier is full. Returns false when the tier has room: keep producing.
	bool BlockSink(const InterruptState &blocked_sink, idx_t batch);

	StreamExecutionResult ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) override;
	unique_ptr<DataChunk> Scan() override;
	void UpdateMinBatchIndex(idx_t min_batch_index);
	bool IsMinimumBatchIndex(lock_guard<mutex> &lock, idx_t batch);
	void CompleteBatch(idx_t batch);
	bool BufferIsEmpty();
	void UnblockSinks() override;
	void WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
	                  StreamQueryResult &result) override;

	inline idx_t ReadQueueCapacity() const {
		return read_queue_capacity;
	}
	inline idx_t BufferCapacity() const {
		return buffer_capacity;
	}
	//! Restarts performed by min-batch advances that freed no bytes (the reclassification edge)
	idx_t MovedNothingRestarts() {
		lock_guard<mutex> lock(glock);
		return moved_nothing_restarts;
	}

private:
	void ResetReplenishState();
	void MoveCompletedBatches(lock_guard<mutex> &lock);
	//! The per-tier parking condition; the caller holds glock
	bool ShouldBlockBatch(lock_guard<mutex> &lock, idx_t batch);
	//! Pop restartable sinks into to_unblock; the caller holds glock
	void CollectRestartableSinks(lock_guard<mutex> &lock, vector<pair<idx_t, InterruptState>> &to_unblock);
	//! Invoke the collected restarts; called outside glock, re-parks the remainder on a throw
	void InvokeUnblocks(const vector<pair<idx_t, InterruptState>> &to_unblock);

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
	//! Counts advances that freed no bytes yet woke a sink; introspection for tests
	idx_t moved_nothing_restarts = 0;
};

} // namespace duckdb
