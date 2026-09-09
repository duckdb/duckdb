#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/query_result.hpp"
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
	shared_ptr<QueryResultNotifier> notifier;
	{
		annotated_lock_guard<annotated_mutex> lock(glock);
		if (lifetime != ResultLifetime::UNDECIDED) {
			return false;
		}
		if (undecided_sinks.empty()) {
			// Only the first park is a transition: the consumer's answer releases all of them
			notifier = result_notifier;
		}
		undecided_sinks.push_back(blocked_sink);
	}
	Signal(notifier);
	return true;
}

bool BufferedData::WaitsOnConsumer() {
	// The undecided list is empty once the retention is settled: ParkUndecided re-checks under glock
	if (lifetime == ResultLifetime::UNDECIDED) {
		annotated_lock_guard<annotated_mutex> lock(glock);
		if (!undecided_sinks.empty()) {
			return true;
		}
	}
	// A space park is only the consumer's to release when a pop is possible: the batched buffer also
	// parks read-ahead batches while the read queue is empty, and the minimum batch releases those
	return HasBlockedSink() && HasObservableChunk();
}

void BufferedData::SetResultNotifier(shared_ptr<QueryResultNotifier> notifier_p) {
	annotated_lock_guard<annotated_mutex> lock(glock);
	result_notifier = std::move(notifier_p);
}

void BufferedData::Signal(const shared_ptr<QueryResultNotifier> &notifier) {
	if (notifier) {
		notifier->Notify();
	}
}

QueryResultState BufferedData::Cancelled(QueryResult &result) {
	return result.Cancelled();
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

QueryResultState BufferedData::ExecuteTaskInternal(QueryResult &result, ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return Cancelled(result);
	}
	if (!cc->IsActiveResult(context_lock, result)) {
		return Cancelled(result);
	}
	DecideDraining();
	// Checked with chunks poppable too, so a cancel ends the drain early. A worker error also raises
	// the flag, so only a flag without an executor error is a cancel; both loads are seq_cst
	const bool interrupted = cc->interrupt_state.load() == ClientInterruptState::INTERRUPTED;
	if (interrupted && !Executor::Get(*cc).HasError()) {
		throw InterruptException();
	}
	if (!interrupted && ReplenishSatisfied()) {
		return QueryResultState::READY;
	}
	UnblockSinks();
	// Let the executor run until the buffer is no longer empty
	auto execution_result = cc->ExecuteTaskInternal(context_lock, result);
	if (execution_result == QueryResultState::EXECUTION_ERROR) {
		// The query has ended, so a still-buffered chunk must not be reported as poppable
		Close();
		return QueryResultState::EXECUTION_ERROR;
	}
	if (ReplenishSatisfied()) {
		return QueryResultState::READY;
	}
	if (execution_result == QueryResultState::BLOCKED || execution_result == QueryResultState::READY) {
		// The engine is waiting on the consumer but nothing is poppable yet: a woken producer has
		// not re-delivered. Its deposit rings the notifier
		return QueryResultState::BLOCKED;
	}
	return execution_result;
}

QueryResultState BufferedData::Pulse(QueryResult &result, ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return Cancelled(result);
	}
	if (!cc->IsActiveResult(context_lock, result)) {
		return Cancelled(result);
	}
	// Checked before the buffer, so a cancel is seen even with chunks poppable. A worker error also
	// raises the flag; only a flag without an executor error is a real cancel
	const bool interrupted = cc->interrupt_state.load() == ClientInterruptState::INTERRUPTED;
	if (interrupted && !Executor::Get(*cc).HasError()) {
		throw InterruptException();
	}
	if (!interrupted && HasObservableChunk()) {
		return QueryResultState::READY;
	}
	// Observe the execution state without running tasks
	auto execution_result = cc->ExecuteTaskInternal(context_lock, result, true);
	if (execution_result == QueryResultState::EXECUTION_ERROR) {
		Close();
		return QueryResultState::EXECUTION_ERROR;
	}
	if (HasObservableChunk()) {
		return QueryResultState::READY;
	}
	if (execution_result == QueryResultState::BLOCKED || execution_result == QueryResultState::READY) {
		return QueryResultState::BLOCKED;
	}
	return execution_result;
}

QueryResultState BufferedData::ReplenishBuffer(QueryResult &result, ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return Cancelled(result);
	}

	QueryResultState execution_result;
	while (!IsObservable(execution_result = ExecuteTaskInternal(result, context_lock))) {
		if (execution_result == QueryResultState::BLOCKED) {
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
