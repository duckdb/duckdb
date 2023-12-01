//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/buffered_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

class BufferedQueryResult;

class BufferedData {
private:
	static constexpr idx_t BUFFER_SIZE = 100000 / STANDARD_VECTOR_SIZE;

public:
	BufferedData(shared_ptr<ClientContext> context) : context(context) {
	}

public:
	void Populate(unique_ptr<DataChunk> chunk);
	unique_ptr<DataChunk> Fetch(BufferedQueryResult &result);
	void AddToBacklog(InterruptState state);
	void ReplenishBuffer(BufferedQueryResult &result);
	bool BufferIsFull() const;

private:
	shared_ptr<ClientContext> context;
	// Our handles to reschedule the blocked sink tasks
	queue<InterruptState> blocked_sinks;
	// Protect against populate/fetch race condition
	mutex glock;
	// Keep track of the size of the buffer to gauge when it should be repopulated
	queue<unique_ptr<DataChunk>> buffered_chunks;
	atomic<idx_t> buffered_chunks_count;
};

} // namespace duckdb
