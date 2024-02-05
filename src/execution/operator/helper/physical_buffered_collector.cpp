#include "duckdb/execution/operator/helper/physical_buffered_collector.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalBufferedCollector::PhysicalBufferedCollector(PreparedStatementData &data, bool parallel)
    : PhysicalResultCollector(data), parallel(parallel) {
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

class BufferedCollectorLocalState : public LocalSinkState {
public:
	bool blocked = false;
};

SinkResultType PhysicalBufferedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedCollectorLocalState>();

	lock_guard<mutex> l(gstate.glock);
	auto &buffered_data = gstate.buffered_data->Cast<SimpleBufferedData>();

	if (!lstate.blocked || buffered_data.BufferIsFull()) {
		lstate.blocked = true;
		auto callback_state = input.interrupt_state;
		auto blocked_sink = BlockedSink(callback_state, chunk.size());
		buffered_data.BlockSink(blocked_sink);
		return SinkResultType::BLOCKED;
	}

	auto to_append = make_uniq<DataChunk>();
	to_append->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
	chunk.Copy(*to_append, 0);
	buffered_data.Append(std::move(to_append));
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalBufferedCollector::Combine(ExecutionContext &context,
                                                         OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalBufferedCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<BufferedCollectorGlobalState>();
	state->context = context.shared_from_this();
	state->buffered_data = make_shared<SimpleBufferedData>(state->context);
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalBufferedCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<BufferedCollectorLocalState>();
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalBufferedCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = state.Cast<BufferedCollectorGlobalState>();
	lock_guard<mutex> l(gstate.glock);
	// FIXME: maybe we want to check if the execution was successfull before creating the StreamQueryResult ?
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
