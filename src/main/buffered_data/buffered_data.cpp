#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

BufferedData::BufferedData(Type type, ClientContext &context_p, bool async)
    : type(type), context(context_p.shared_from_this()), async(async) {
	auto &config = ClientConfig::GetConfig(context_p);
	// The setting has no lower bound; a buffer that can never admit a chunk parks every sink
	// on an "always full" empty buffer and the stream silently ends with zero rows
	total_buffer_size = MaxValue<idx_t>(config.streaming_buffer_size, 1);
}

BufferedData::~BufferedData() {
}

StreamExecutionResult BufferedData::MapExecutionResult(PendingExecutionResult execution_result) {
	switch (execution_result) {
	case PendingExecutionResult::BLOCKED:
	case PendingExecutionResult::RESULT_READY:
		return StreamExecutionResult::BLOCKED;
	case PendingExecutionResult::NO_TASKS_AVAILABLE:
	case PendingExecutionResult::RESULT_NOT_READY:
		return StreamExecutionResult::CHUNK_NOT_READY;
	case PendingExecutionResult::EXECUTION_FINISHED:
		return StreamExecutionResult::EXECUTION_FINISHED;
	case PendingExecutionResult::EXECUTION_ERROR:
		return StreamExecutionResult::EXECUTION_ERROR;
	default:
		throw InternalException("No conversion from PendingExecutionResult (%s) -> StreamExecutionResult",
		                        EnumUtil::ToString(execution_result));
	}
}

void BufferedData::WaitForChunk(ClientContext &client_context, ClientContextLock &context_lock,
                                StreamQueryResult &result) {
	client_context.WaitForProgress(context_lock, result);
}

void BufferedData::SignalChunkAvailable(const shared_ptr<QueryResultNotifier> &notifier) {
	chunk_ready_cv.notify_all();
	if (notifier) {
		notifier->Notify();
	}
}

idx_t BufferedData::LowWaterMark(idx_t capacity) {
	return MaxValue<idx_t>(capacity / 2, 1);
}

StreamExecutionResult BufferedData::ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}

	StreamExecutionResult execution_result;
	while (!StreamQueryResult::IsChunkReady(execution_result = ExecuteTaskInternal(result, context_lock))) {
		if (execution_result == StreamExecutionResult::BLOCKED) {
			UnblockSinks();
		}
		if (async) {
			// Blocking fetch on an async result: wait, do not spin. WaitForTask returns
			// immediately while the producer queue is non-empty, which means nothing for
			// a consumer that never runs tasks, so BLOCKED must wait here too.
			WaitForChunk(*cc, context_lock, result);
		} else if (execution_result == StreamExecutionResult::BLOCKED) {
			cc->WaitForTask(context_lock, result);
		}
	}
	if (result.HasError()) {
		Close();
	}
	return execution_result;
}

} // namespace duckdb
