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
	//! The chunks that make up the batch
	deque<unique_ptr<DataChunk>> chunks;
	//! Whether the batch is completed (NextBatch has been called)
	bool completed = false;
};

//! Shared base of the batched (order-preserving) streaming buffer. The subclasses
//! differ only in who drives execution (see StreamingExecutionMode).
class BatchedBufferedData : public BufferedData {
public:
	static constexpr const BufferedData::Type TYPE = BufferedData::Type::BATCHED;

protected:
	BatchedBufferedData(ClientContext &context, bool async);

public:
	//! Buffer a copy of the chunk, or block the sink when the chunk does not fit.
	//! The decision and the byte accounting share one critical section, so racing
	//! producers cannot push the buffered bytes past the cap. Returns true on block.
	bool AppendOrBlock(const DataChunk &chunk, idx_t batch, const InterruptState &blocked_sink);

	unique_ptr<DataChunk> Scan() override;
	void UpdateMinBatchIndex(idx_t min_batch_index);
	bool IsMinimumBatchIndex(annotated_lock_guard<annotated_mutex> &lock, idx_t batch) DUCKDB_REQUIRES(glock);
	void CompleteBatch(idx_t batch);
	bool BufferIsEmpty();
	bool HasObservableChunk() override {
		return !BufferIsEmpty();
	}
	void UnblockSinks() override;
	void AssertNoBlockedSinks() override;

	inline idx_t ReadQueueCapacity() const {
		return read_queue_capacity;
	}
	inline idx_t BufferCapacity() const {
		return buffer_capacity;
	}
	//! Restarts performed by minimum batch advances that freed no bytes
	idx_t MovedNothingRestarts() {
		annotated_lock_guard<annotated_mutex> lock(glock);
		return moved_nothing_restarts;
	}
	//! The highest number of bytes the buffer ever held
	idx_t PeakBufferedBytes() override {
		annotated_lock_guard<annotated_mutex> lock(glock);
		return peak_buffered_bytes;
	}

protected:
	void ResetReplenishState();
	void MoveCompletedBatches(annotated_lock_guard<annotated_mutex> &lock) DUCKDB_REQUIRES(glock);
	//! Whether a chunk of incoming_bytes may not be buffered for this batch right now.
	//! An oversized chunk is admitted only into an empty pool, so the stream keeps progressing
	virtual bool ShouldBlockBatch(annotated_lock_guard<annotated_mutex> &lock, idx_t batch, idx_t incoming_bytes)
	    DUCKDB_REQUIRES(glock);
	//! Whether a pop can newly make a blocked sink eligible for restart. The caller holds glock
	virtual bool PopMayFreeSinks(annotated_lock_guard<annotated_mutex> &lock) DUCKDB_REQUIRES(glock);
	//! Pop restartable sinks into to_unblock. The caller holds glock
	void CollectRestartableSinks(annotated_lock_guard<annotated_mutex> &lock,
	                             vector<pair<idx_t, BlockedSink>> &to_unblock) DUCKDB_REQUIRES(glock);
	//! Deposit a restarting sink's parked copy. The caller holds glock
	void DepositParked(annotated_lock_guard<annotated_mutex> &lock, idx_t batch, BlockedSink &sink)
	    DUCKDB_REQUIRES(glock);
	//! Invoke the collected restarts. The callbacks take the executor lock, so they must
	//! run outside glock. When one throws, the remaining sinks are blocked again
	void InvokeUnblocks(vector<pair<idx_t, BlockedSink>> &to_unblock) DUCKDB_EXCLUDES(glock);

protected:
	//! The buffer where chunks are written before they are ready to be read.
	map<idx_t, InProgressBatch> buffer DUCKDB_GUARDED_BY(glock);
	idx_t buffer_capacity;
	atomic<idx_t> buffer_byte_count;

	//! The queue containing the chunks that can be read.
	deque<unique_ptr<DataChunk>> read_queue DUCKDB_GUARDED_BY(glock);
	idx_t read_queue_capacity;
	atomic<idx_t> read_queue_byte_count;

	map<idx_t, BlockedSink> blocked_sinks DUCKDB_GUARDED_BY(glock);

	idx_t min_batch DUCKDB_GUARDED_BY(glock);
	//! Debug variable to verify that order is preserved correctly.
	idx_t lowest_moved_batch DUCKDB_GUARDED_BY(glock) = 0;
	//! Counts advances that freed no bytes yet woke a sink
	idx_t moved_nothing_restarts DUCKDB_GUARDED_BY(glock) = 0;
	//! The highest number of bytes ever buffered
	idx_t peak_buffered_bytes DUCKDB_GUARDED_BY(glock) = 0;
	//! The largest single chunk seen so far. Sizes the async reserve for the minimum batch
	idx_t max_seen_chunk_bytes DUCKDB_GUARDED_BY(glock) = 0;
};

//! Sync subclass. The fetching thread unblocks sinks and executes tasks itself
//! whenever the read queue is empty.
class BatchedBufferedDataSync : public BatchedBufferedData {
public:
	explicit BatchedBufferedDataSync(ClientContext &context);

public:
	StreamExecutionResult ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) override;
};

//! Async subclass. Fetching never executes tasks, so the buffer restarts blocked
//! sinks itself. Blocking checks one shared byte budget, with a reserve for the
//! minimum batch. A minimum batch producer only blocks while chunks are poppable.
class BatchedBufferedDataAsync : public BatchedBufferedData {
public:
	explicit BatchedBufferedDataAsync(ClientContext &context);

public:
	StreamExecutionResult ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) override;
	void WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
	                  StreamQueryResult &result) override;

protected:
	bool ShouldBlockBatch(annotated_lock_guard<annotated_mutex> &lock, idx_t batch, idx_t incoming_bytes) override
	    DUCKDB_REQUIRES(glock);
	bool PopMayFreeSinks(annotated_lock_guard<annotated_mutex> &lock) override DUCKDB_REQUIRES(glock);
};

} // namespace duckdb
