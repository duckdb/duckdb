#include "duckdb/execution/operator/helper/physical_buffered_collector.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/main/buffered_query_result.hpp"
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
	shared_ptr<ClientContext> context;
	shared_ptr<BufferedData> buffered_data;
};

class BufferedCollectorLocalState : public LocalSinkState {
public:
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
	bool blocked = false;
};

SinkResultType PhysicalBufferedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<BufferedCollectorLocalState>();

	lock_guard<mutex> l(gstate.glock);
	auto &buffered_data = *gstate.buffered_data;

	if (!lstate.blocked) {
		// Always block the first time
		lstate.blocked = true;
		auto callback_state = input.interrupt_state;
		auto blocked_sink = BlockedSink {/* state = */ callback_state,
		                                 /* chunk_size = */ chunk.size()};
		buffered_data.AddToBacklog(blocked_sink);
		return SinkResultType::BLOCKED;
	}

	if (buffered_data.BufferIsFull()) {
		// Block again when we've already buffered enough chunks
		auto callback_state = input.interrupt_state;
		auto blocked_sink = BlockedSink {/* state = */ callback_state,
		                                 /* chunk_size = */ chunk.size()};
		buffered_data.AddToBacklog(blocked_sink);
		return SinkResultType::BLOCKED;
	}
	auto to_append = make_uniq<DataChunk>();
	to_append->InitializeEmpty(chunk.GetTypes());
	to_append->Reference(chunk);
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
	state->collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	state->collection->InitializeAppend(state->append_state);
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalBufferedCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = state.Cast<BufferedCollectorGlobalState>();
	lock_guard<mutex> l(gstate.glock);
	auto result = make_uniq<BufferedQueryResult>(statement_type, properties, types, names,
	                                             gstate.context->GetClientProperties(), gstate.buffered_data);
	return std::move(result);
}

bool PhysicalBufferedCollector::ParallelSink() const {
	return parallel;
}

bool PhysicalBufferedCollector::SinkOrderDependent() const {
	return true;
}

} // namespace duckdb
