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
#include "duckdb/common/enums/pending_execution_result.hpp"

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
protected:
	enum class Type { SIMPLE };

public:
	BufferedData(Type type, weak_ptr<ClientContext> context) : type(type), context(context) {
	}
	virtual ~BufferedData() {
	}

public:
	virtual bool BufferIsFull() = 0;
	virtual PendingExecutionResult ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) = 0;
	virtual unique_ptr<DataChunk> Scan() = 0;
	shared_ptr<ClientContext> GetContext() {
		return context.lock();
	}
	bool Closed() const {
		if (context.expired()) {
			return false;
		}
		auto c = context.lock();
		return c == nullptr;
	}
	void Close() {
		context.reset();
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		if (TARGET::TYPE != type) {
			throw InternalException("Failed to cast buffered data to type - buffered data type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (TARGET::TYPE != type) {
			throw InternalException("Failed to cast buffered data to type - buffered data type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	Type type;
	//! This is weak to avoid a cyclical reference
	weak_ptr<ClientContext> context;
	//! Protect against populate/fetch race condition
	mutex glock;
};

} // namespace duckdb
