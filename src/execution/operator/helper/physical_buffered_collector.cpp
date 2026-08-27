#include "duckdb/execution/operator/helper/physical_buffered_collector.hpp"
#include "duckdb/common/query_parameters.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalBufferedCollector::PhysicalBufferedCollector(PhysicalPlan &physical_plan, PreparedStatementData &data,
                                                     bool parallel)
    : PhysicalResultCollector(physical_plan, data), parallel(parallel),
      async(data.execution_mode == StreamingExecutionMode::ASYNC) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BufferedCollectorGlobalState : public GlobalSinkState {
public:
	//! This is weak to avoid creating a cyclical reference
	weak_ptr<ClientContext> context;
	//! Set once, before execution starts. The buffer synchronizes itself.
	shared_ptr<BufferedData> buffered_data;
};

class BufferedCollectorLocalState : public LocalSinkState {
public:
	//! The parked chunk is deposited by restart selection. The re-entered Sink must not append it again
	bool chunk_deposited = false;
};

SinkResultType PhysicalBufferedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedCollectorLocalState>();
	if (lstate.chunk_deposited) {
		lstate.chunk_deposited = false;
		return SinkResultType::NEED_MORE_INPUT;
	}
	auto &buffered_data = gstate.buffered_data->Cast<SimpleBufferedData>();
	if (buffered_data.AppendOrBlock(chunk, input.interrupt_state)) {
		lstate.chunk_deposited = true;
		return SinkResultType::BLOCKED;
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
	Executor::Get(context).SetStreamingBufferedData(*state->buffered_data);
	if (async) {
		// A notify callback passed at execution time is set before this buffer exists
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
