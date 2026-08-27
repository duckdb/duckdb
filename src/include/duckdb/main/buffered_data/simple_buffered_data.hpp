//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/simple_buffered_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

class StreamQueryResult;
class ClientContextLock;

class SimpleBufferedData : public BufferedData {
public:
	static constexpr const BufferedData::Type TYPE = BufferedData::Type::SIMPLE;

public:
	explicit SimpleBufferedData(ClientContext &context, bool async = false);
	~SimpleBufferedData() override;

public:
	//! Buffer a copy of the chunk, or block the sink when the chunk does not fit.
	//! The cap is never exceeded, except that one oversized chunk is admitted into an
	//! empty buffer. Returns true on block. Safe to call from parallel producers.
	bool AppendOrBlock(const DataChunk &chunk, const InterruptState &blocked_sink);
	//! Whether the buffer can accept no further chunk right now. That is the case at
	//! the cap, and when a producer already blocked because its chunk did not fit
	bool BufferSaturated();
	//! The highest number of bytes the buffer ever held
	idx_t PeakBufferedBytes() override;
	//! Whether a chunk is buffered. Readiness and notification both check the chunk
	//! queue, never the byte count. Chunks with rows but zero data bytes exist.
	bool HasBufferedChunk();
	bool HasObservableChunk() override {
		return HasBufferedChunk();
	}
	void UnblockSinks() override;
	void AssertNoBlockedSinks() override;
	StreamExecutionResult ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) override;
	unique_ptr<DataChunk> Scan() override;
	void WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
	                  StreamQueryResult &result) override;
	inline idx_t BufferSize() const {
		return buffer_size;
	}

private:
	//! Pop restartable sinks into to_unblock. The caller holds glock
	void CollectRestartableSinks(vector<BlockedSink> &to_unblock);
	//! Invoke the collected restarts. Called outside glock. When one throws, the
	//! remaining sinks are blocked again
	void InvokeUnblocks(vector<BlockedSink> &to_unblock);

private:
	//! Our handles to reschedule the blocked sink tasks
	queue<BlockedSink> blocked_sinks;
	//! The queue of chunks
	queue<unique_ptr<DataChunk>> buffered_chunks;
	//! The bytes currently buffered
	atomic<idx_t> buffered_count;
	//! The byte cap of the buffer
	idx_t buffer_size;
	//! The highest number of bytes ever buffered
	idx_t peak_buffered_bytes = 0;
};

} // namespace duckdb
