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
#include "duckdb/common/enums/stream_execution_result.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/query_result_notifier.hpp"

#include <condition_variable>

namespace duckdb {

class StreamQueryResult;
class ClientContextLock;

class BufferedData {
protected:
	enum class Type { SIMPLE, BATCHED };

public:
	BufferedData(Type type, ClientContext &context, bool async = false);
	virtual ~BufferedData();

public:
	StreamExecutionResult ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock);
	virtual StreamExecutionResult ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) = 0;
	virtual unique_ptr<DataChunk> Scan() = 0;
	virtual void UnblockSinks() = 0;
	//! Blocking, bounded wait until a chunk may be available; the base waits for generic progress
	virtual void WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
	                          StreamQueryResult &result);
	shared_ptr<ClientContext> GetContext() {
		return context.lock();
	}
	//! Whether background workers keep this buffer filled (see QueryResultExecutionMode)
	bool IsAsync() const {
		return async;
	}
	//! Set the notifier called when the buffer turns non-empty (async streaming results)
	void SetResultNotifier(shared_ptr<QueryResultNotifier> result_notifier_p) {
		lock_guard<mutex> guard(glock);
		result_notifier = std::move(result_notifier_p);
	}
	shared_ptr<QueryResultNotifier> GetResultNotifier() {
		lock_guard<mutex> guard(glock);
		return result_notifier;
	}
	bool Closed() const {
		if (context.expired()) {
			return true;
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
	static StreamExecutionResult MapExecutionResult(PendingExecutionResult execution_result);
	//! Wake both chunk-ready audiences: the cv (blocking fetches) and the notifier (async
	//! consumers). Call outside glock, with the notifier captured under it on the same edge.
	//! The simple buffer cannot use this: its collector's Sink holds an outer lock, so it
	//! fires the cv inline and returns the notifier for the collector to fire instead.
	void SignalChunkAvailable(const shared_ptr<QueryResultNotifier> &notifier);
	//! Restart-batching mark for the pop edge; the floor keeps it satisfiable for tiny buffers
	static idx_t LowWaterMark(idx_t capacity);

protected:
	Type type;
	//! This is weak to avoid a cyclical reference
	weak_ptr<ClientContext> context;
	//! The maximum amount of memory we should keep buffered
	idx_t total_buffer_size;
	//! Whether background workers keep the buffer filled (async results)
	bool async;
	//! Called when the buffer turns non-empty (may be null). Guarded by glock.
	shared_ptr<QueryResultNotifier> result_notifier;
	//! Signalled when the buffer turns non-empty, for blocking async fetches
	std::condition_variable chunk_ready_cv;
	//! Protect against populate/fetch race condition
	mutex glock;
};

} // namespace duckdb
