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
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

namespace duckdb {

class StreamQueryResult;
class ClientContextLock;

struct BlockedSink {
public:
	BlockedSink(InterruptState state, idx_t chunk_size) : state(state), chunk_size(chunk_size) {
	}

public:
	//! The handle to reschedule the blocked sink
	InterruptState state;
	//! The amount of tuples this sink would add
	idx_t chunk_size;
};

class BufferedData {
private:
	//! (roughly) The max amount of tuples we'll keep buffered at a time
	static constexpr idx_t BUFFER_SIZE = 100000;

public:
	BufferedData(shared_ptr<ClientContext> context) : context(context) {
	}
	virtual ~BufferedData() {
	}

public:
	virtual bool BufferIsFull() = 0;
	virtual void ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) = 0;
	virtual unique_ptr<DataChunk> Scan() = 0;
	shared_ptr<ClientContext> GetContext() {
		return context;
	}
	bool Closed() const {
		return !context;
	}
	void Close() {
		context = nullptr;
	}

protected:
	shared_ptr<ClientContext> context;
	//! Protect against populate/fetch race condition
	mutex glock;
};

} // namespace duckdb
