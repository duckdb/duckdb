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

struct BufferedDataScanState {
	//! The chunk we're currently scanning from
	unique_ptr<DataChunk> chunk = nullptr;
	//! The offset into the current chunk
	idx_t offset = 0;
};

class BufferedData {
private:
	//! (roughly) The max amount of tuples we'll keep buffered at a time
	static constexpr idx_t BUFFER_SIZE = 100000;

public:
	BufferedData(shared_ptr<ClientContext> context) : context(context) {
	}

public:
	void Append(unique_ptr<DataChunk> chunk);

	unique_ptr<DataChunk> Fetch(BufferedQueryResult &result);
	void AddToBacklog(InterruptState state);
	void ReplenishBuffer(BufferedQueryResult &result);
	bool BufferIsFull() const;

private:
	unique_ptr<DataChunk> Scan();

private:
	shared_ptr<ClientContext> context;
	//! Our handles to reschedule the blocked sink tasks
	queue<InterruptState> blocked_sinks;
	//! Protect against populate/fetch race condition
	mutex glock;
	//! The queue of chunks
	queue<unique_ptr<DataChunk>> buffered_chunks;
	//! The current capacity of the buffer (tuples)
	atomic<idx_t> buffered_count;
	//! Scan state
	BufferedDataScanState scan_state;
};

} // namespace duckdb
