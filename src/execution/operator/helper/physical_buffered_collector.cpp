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
};

SinkResultType PhysicalBufferedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedCollectorGlobalState>();

	lock_guard<mutex> l(gstate.glock);
	auto callback_state = input.interrupt_state;
	gstate.buffered_data->AddToBacklog(callback_state);

	return SinkResultType::BLOCKED;
}

SinkCombineResultType PhysicalBufferedCollector::Combine(ExecutionContext &context,
                                                         OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalBufferedCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<BufferedCollectorGlobalState>();
	state->context = context.shared_from_this();
	state->buffered_data = make_shared<BufferedData>();
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
