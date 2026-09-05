#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

BufferedData::BufferedData(Type type, ClientContext &context_p, ResultLifetime lifetime)
    : type(type), context(context_p.shared_from_this()),
      // The setting has no lower bound. A buffer that can never admit a chunk blocks
      // every sink while empty, and the stream silently ends with zero rows
      total_buffer_size(MaxValue<idx_t>(ClientConfig::GetConfig(context_p).max_streaming_buffer_size, 1)),
      lifetime(lifetime) {
}

BufferedData::~BufferedData() {
}

ResultLifetime BufferedData::Decide(ResultLifetime decision) {
	D_ASSERT(decision != ResultLifetime::UNDECIDED);
	// Decided once and never changed, so a settled buffer answers without the lock
	if (lifetime != ResultLifetime::UNDECIDED) {
		return lifetime;
	}
	vector<InterruptState> to_wake;
	{
		annotated_lock_guard<annotated_mutex> lock(glock);
		if (lifetime == ResultLifetime::UNDECIDED) {
			lifetime = decision;
		}
		to_wake = std::move(undecided_sinks);
		undecided_sinks.clear();
	}
	// Callback() takes the executor lock, so the wake runs outside glock
	for (auto &state : to_wake) {
		state.Callback();
	}
	return lifetime;
}

void BufferedData::DecideDraining() {
	if (Decide(ResultLifetime::DRAINING) != ResultLifetime::DRAINING) {
		throw InvalidInputException("Cannot fetch from a stream result that is being materialized");
	}
}

bool BufferedData::ParkUndecided(const InterruptState &blocked_sink) {
	annotated_lock_guard<annotated_mutex> lock(glock);
	if (lifetime != ResultLifetime::UNDECIDED) {
		return false;
	}
	undecided_sinks.push_back(blocked_sink);
	return true;
}

bool BufferedData::HasParkedProducer() {
	// The undecided list is empty once the retention is settled: ParkUndecided re-checks under glock
	if (lifetime == ResultLifetime::UNDECIDED) {
		annotated_lock_guard<annotated_mutex> lock(glock);
		if (!undecided_sinks.empty()) {
			return true;
		}
	}
	return HasBlockedSink();
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

unique_ptr<DataChunk> BufferedData::CopyForBuffering(DataChunk &chunk) {
	auto copy = make_uniq<DataChunk>();
	copy->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes(), MaxValue<idx_t>(chunk.size(), 1));
	chunk.Copy(*copy, 0);
	return copy;
}

idx_t BufferedData::LowWaterMark(idx_t capacity) {
	return MaxValue<idx_t>(capacity / 2, 1);
}

StreamExecutionResult BufferedData::ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}
	if (!cc->IsActiveResult(context_lock, result)) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}
	DecideDraining();
	// Checked with chunks poppable too, so a cancel ends the drain early. A worker error also raises
	// the flag, so only a flag without an executor error is a cancel; both loads are seq_cst
	const bool interrupted = cc->interrupt_state.load() == ClientInterruptState::INTERRUPTED;
	if (interrupted && !Executor::Get(*cc).HasError()) {
		throw InterruptException();
	}
	if (!interrupted && ReplenishSatisfied()) {
		return StreamExecutionResult::CHUNK_READY;
	}
	UnblockSinks();
	// Let the executor run until the buffer is no longer empty
	auto execution_result = cc->ExecuteTaskInternal(context_lock, result);
	if (execution_result == PendingExecutionResult::EXECUTION_ERROR) {
		// The query has ended, so a still-buffered chunk must not be reported as poppable
		Close();
		return StreamExecutionResult::EXECUTION_ERROR;
	}
	if (ReplenishSatisfied()) {
		return StreamExecutionResult::CHUNK_READY;
	}
	if (execution_result == PendingExecutionResult::BLOCKED ||
	    execution_result == PendingExecutionResult::RESULT_READY) {
		return StreamExecutionResult::BLOCKED;
	}
	return MapExecutionResult(execution_result);
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
			cc->WaitForTask(context_lock, result);
		}
	}
	if (result.HasError()) {
		Close();
	}
	return execution_result;
}

} // namespace duckdb
