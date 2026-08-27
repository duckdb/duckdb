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
#include "duckdb/common/enums/streaming_execution_mode.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/main/query_result_notifier.hpp"

#include <condition_variable>

namespace duckdb {

class StreamQueryResult;
class ClientContextLock;

//! A blocked sink. Holds the reschedule handle and the finished copy of the chunk it
//! could not append. Restart selection only wakes the sink when that chunk fits, and
//! deposits the copy before the wake, so the chunk needs no producer time to turn visible
struct BlockedSink {
public:
	InterruptState state;
	idx_t pending_bytes;
	unique_ptr<DataChunk> pending_chunk;
};

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
	//! Blocking, bounded wait until a chunk may be available. The base waits for generic progress
	virtual void WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
	                          StreamQueryResult &result);
	shared_ptr<ClientContext> GetContext() {
		return context.lock();
	}
	//! Whether background workers keep this buffer filled (see StreamingExecutionMode)
	bool IsAsync() const {
		return async;
	}
	//! Set the notifier called when the buffer turns non-empty (async streaming results)
	void SetResultNotifier(shared_ptr<QueryResultNotifier> result_notifier_p) {
		annotated_lock_guard<annotated_mutex> guard(glock);
		result_notifier = std::move(result_notifier_p);
	}
	shared_ptr<QueryResultNotifier> GetResultNotifier() {
		annotated_lock_guard<annotated_mutex> guard(glock);
		return result_notifier;
	}
	//! The highest number of bytes the buffer ever held
	virtual idx_t PeakBufferedBytes() = 0;
	//! Whether a chunk is ready for the consumer to pop
	virtual bool HasObservableChunk() = 0;
	//! A blocked sink still holds an undelivered chunk, so none can survive a clean end
	//! of stream. Compiled away without assertions.
	virtual void AssertNoBlockedSinks() = 0;
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
	//! Signal the cv (blocking fetches) and the notifier (async consumers) that a chunk is
	//! available. Capture the notifier under glock when appending.
	void SignalChunkAvailable(const shared_ptr<QueryResultNotifier> &notifier) DUCKDB_EXCLUDES(glock);
	//! Pops below this mark restart blocked sinks. The floor keeps it reachable for tiny buffers
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
	annotated_mutex glock;
};

} // namespace duckdb
