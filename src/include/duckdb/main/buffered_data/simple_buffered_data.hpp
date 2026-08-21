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
	//! Buffer a copy of the chunk. Returns the notifier for the caller to invoke after
	//! releasing its locks, or null when no notification is due.
	shared_ptr<QueryResultNotifier> Append(const DataChunk &chunk);
	//! Park a full-buffer sink. Returns false when a concurrent pop made room: keep producing.
	bool BlockSink(const InterruptState &blocked_sink);
	bool BufferIsFull();
	//! Whether a chunk is buffered. Readiness and the notify edge both use the chunk queue,
	//! never the byte count: chunks with rows but zero data bytes exist.
	bool HasBufferedChunk();
	void UnblockSinks() override;
	StreamExecutionResult ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) override;
	unique_ptr<DataChunk> Scan() override;
	void WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
	                  StreamQueryResult &result) override;
	inline idx_t BufferSize() const {
		return buffer_size;
	}

private:
	//! Pop restartable sinks into to_unblock; the caller holds glock
	void CollectRestartableSinks(vector<InterruptState> &to_unblock);
	//! Invoke the collected restarts; called outside glock, re-parks the remainder on a throw
	void InvokeUnblocks(const vector<InterruptState> &to_unblock);

private:
	//! Our handles to reschedule the blocked sink tasks
	queue<InterruptState> blocked_sinks;
	//! The queue of chunks
	queue<unique_ptr<DataChunk>> buffered_chunks;
	//! The current capacity of the buffer (tuples)
	atomic<idx_t> buffered_count;
	//! The amount of tuples we should buffer
	idx_t buffer_size;
};

} // namespace duckdb
