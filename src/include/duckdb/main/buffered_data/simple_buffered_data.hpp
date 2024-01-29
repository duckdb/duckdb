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
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

class StreamQueryResult;
class ClientContextLock;

class SimpleBufferedData : public BufferedData {
public:
	static constexpr const BufferedData::Type TYPE = BufferedData::Type::SIMPLE;

private:
	//! (roughly) The max amount of tuples we'll keep buffered at a time
	static constexpr idx_t BUFFER_SIZE = 100000;

public:
	SimpleBufferedData(weak_ptr<ClientContext> context);
	~SimpleBufferedData() override;

public:
	void Append(unique_ptr<DataChunk> chunk);
	void BlockSink(const BlockedSink &blocked_sink);
	bool BufferIsFull() override;
	PendingExecutionResult ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) override;
	unique_ptr<DataChunk> Scan() override;

private:
	void UnblockSinks();

private:
	//! Our handles to reschedule the blocked sink tasks
	queue<BlockedSink> blocked_sinks;
	//! The queue of chunks
	queue<unique_ptr<DataChunk>> buffered_chunks;
	//! The current capacity of the buffer (tuples)
	atomic<idx_t> buffered_count;
};

} // namespace duckdb
