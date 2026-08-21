#include "duckdb/execution/operator/helper/physical_buffered_collector.hpp"
#include "duckdb/common/query_parameters.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalBufferedCollector::PhysicalBufferedCollector(PhysicalPlan &physical_plan, PreparedStatementData &data,
                                                     bool parallel)
    : PhysicalResultCollector(physical_plan, data), parallel(parallel),
      async(data.execution_mode == QueryResultExecutionMode::ASYNC) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BufferedCollectorGlobalState : public GlobalSinkState {
public:
	mutex glock;
	//! This is weak to avoid creating a cyclical reference
	weak_ptr<ClientContext> context;
	shared_ptr<BufferedData> buffered_data;
};

class BufferedCollectorLocalState : public LocalSinkState {};

SinkResultType PhysicalBufferedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedCollectorLocalState>();
	(void)lstate;

	shared_ptr<QueryResultNotifier> notifier;
	{
		lock_guard<mutex> l(gstate.glock);
		auto &buffered_data = gstate.buffered_data->Cast<SimpleBufferedData>();

		if (buffered_data.BufferIsFull()) {
			auto callback_state = input.interrupt_state;
			if (buffered_data.BlockSink(callback_state)) {
				return SinkResultType::BLOCKED;
			}
			// Raced with a concurrent pop: the buffer has room again, keep producing.
		}
		notifier = buffered_data.Append(chunk);
	}
	if (notifier) {
		// Notify outside gstate.glock: the callback must not run under a lock sinks serialize on
		notifier->Notify();
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalBufferedCollector::Combine(ExecutionContext &context,
                                                         OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalBufferedCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<BufferedCollectorGlobalState>();
	state->context = context.shared_from_this();
	state->buffered_data = make_shared_ptr<SimpleBufferedData>(context, async);
	if (async) {
		// A notify callback passed at execution time is armed before this buffer exists
		auto notifier = context.GetActiveResultNotifier();
		if (notifier) {
			state->buffered_data->SetResultNotifier(std::move(notifier));
		}
	}
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalBufferedCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<BufferedCollectorLocalState>();
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalBufferedCollector::GetResult(GlobalSinkState &state) const {
	auto &gstate = state.Cast<BufferedCollectorGlobalState>();
	lock_guard<mutex> l(gstate.glock);
	// FIXME: maybe we want to check if the execution was successful before creating the StreamQueryResult ?
	auto cc = gstate.context.lock();
	auto result = make_uniq<StreamQueryResult>(statement_type, properties, types, names, cc->GetClientProperties(),
	                                           gstate.buffered_data);
	return std::move(result);
}

bool PhysicalBufferedCollector::ParallelSink() const {
	return parallel;
}

bool PhysicalBufferedCollector::SinkOrderDependent() const {
	return true;
}

} // namespace duckdb
