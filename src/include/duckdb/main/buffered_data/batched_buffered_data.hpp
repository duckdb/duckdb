//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/batched_buffered_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/thread_annotation.hpp"
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
	//! The batch's chunks, in arrival order.
	vector<BufferedChunk> chunk_refs;
	//! Whether the batch is completed (CompleteBatch has been called).
	bool completed = false;
};

//! The order-preserving streaming buffer: chunks wait in `buffer` until their batch becomes the minimum, then move
//! to the `read_queue` the consumer pops. A minimum-batch producer only parks while chunks are poppable, so a pop
//! always wakes it
class BatchedBufferedData : public BufferedData {
public:
	static constexpr const BufferedData::Type TYPE = BufferedData::Type::BATCHED;

public:
	BatchedBufferedData(ClientContext &context, ResultLifetime lifetime);

public:
	//! Buffer a copy of the chunk, or block the sink when the chunk does not fit. Returns true on block.
	bool AppendOrBlock(DataChunk &chunk, idx_t batch, const InterruptState &blocked_sink);

	unique_ptr<DataChunk> Scan() override;
	void UpdateMinBatchIndex(idx_t min_batch_index);
	bool IsMinimumBatchIndex(annotated_lock_guard<annotated_mutex> &lock, idx_t batch) DUCKDB_REQUIRES(glock);
	void CompleteBatch(idx_t batch);
	bool BufferIsEmpty();
	bool HasBlockedSink() override {
		annotated_lock_guard<annotated_mutex> lock(glock);
		return !blocked_sinks.empty();
	}
	void UnblockSinks() override;
	void AssertNoBlockedSinks() override;

	//! The highest number of bytes the buffer ever held. Used for reserving space and for profiling.
	idx_t PeakBufferedBytes() override {
		annotated_lock_guard<annotated_mutex> lock(glock);
		return peak_buffered_bytes;
	}

protected:
	//! Consumption order is the read queue, so the replenish stops once it holds a chunk
	bool ReplenishSatisfied() override {
		return !BufferIsEmpty();
	}
	void MoveCompletedBatches(annotated_lock_guard<annotated_mutex> &lock) DUCKDB_REQUIRES(glock);
	//! Whether a chunk of incoming_bytes may not be buffered for this batch right now.
	bool ShouldBlockBatch(annotated_lock_guard<annotated_mutex> &lock, idx_t batch, idx_t incoming_bytes)
	    DUCKDB_REQUIRES(glock);
	//! Pop restartable sinks into to_unblock.
	void CollectRestartableSinks(annotated_lock_guard<annotated_mutex> &lock,
	                             vector<pair<idx_t, BlockedSink>> &to_unblock) DUCKDB_REQUIRES(glock);
	//! Deposit a restarting sink's parked chunk.
	void DepositParked(annotated_lock_guard<annotated_mutex> &lock, idx_t batch, BlockedSink &sink)
	    DUCKDB_REQUIRES(glock);
	//! Invoke the collected restartable sinks in to_unblock.
	void InvokeUnblocks(const vector<pair<idx_t, BlockedSink>> &to_unblock) DUCKDB_EXCLUDES(glock);

protected:
	//! The buffer where chunks are written before they are ready to be read.
	map<idx_t, InProgressBatch> buffer DUCKDB_GUARDED_BY(glock);
	idx_t buffer_byte_count DUCKDB_GUARDED_BY(glock);

	//! The readable chunks, in consumption order
	queue<BufferedChunk> read_queue DUCKDB_GUARDED_BY(glock);
	idx_t read_queue_byte_count DUCKDB_GUARDED_BY(glock);

	map<idx_t, BlockedSink> blocked_sinks DUCKDB_GUARDED_BY(glock);

	idx_t min_batch DUCKDB_GUARDED_BY(glock);
	//! Debug variable to verify that order is preserved correctly.
	idx_t lowest_moved_batch DUCKDB_GUARDED_BY(glock) = 0;
	//! The highest number of bytes ever buffered
	idx_t peak_buffered_bytes DUCKDB_GUARDED_BY(glock) = 0;
	//! The largest single chunk seen so far
	idx_t max_seen_chunk_bytes DUCKDB_GUARDED_BY(glock) = 0;
};

} // namespace duckdb
