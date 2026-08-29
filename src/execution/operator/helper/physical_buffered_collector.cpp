#include "duckdb/execution/operator/helper/physical_buffered_collector.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"

namespace duckdb {

PhysicalBufferedCollector::PhysicalBufferedCollector(PhysicalPlan &physical_plan, PreparedStatementData &data,
                                                     bool parallel)
    : PhysicalResultCollector(physical_plan, data), parallel(parallel) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BufferedCollectorGlobalState : public GlobalSinkState {
public:
	explicit BufferedCollectorGlobalState(ClientContext &context)
	    : buffered_data(make_shared_ptr<SimpleBufferedData>(context)) {
	}

	//! Serializes the capacity check and append for parallel sinks
	mutex glock;
	shared_ptr<SimpleBufferedData> buffered_data;
};

SinkResultType PhysicalBufferedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BufferedCollectorGlobalState>();

	lock_guard<mutex> l(gstate.glock);
	auto &buffered_data = *gstate.buffered_data;

	if (buffered_data.BufferIsFull()) {
		buffered_data.BlockSink(input.interrupt_state);
		return SinkResultType::BLOCKED;
	}
	buffered_data.Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalBufferedCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BufferedCollectorGlobalState>(context);
}

unique_ptr<QueryResult> PhysicalBufferedCollector::GetResult(GlobalSinkState &state) const {
	auto &gstate = state.Cast<BufferedCollectorGlobalState>();
	auto context = gstate.buffered_data->GetContext();
	if (!context) {
		throw ConnectionException("Connection has already been closed");
	}
	return make_uniq<StreamQueryResult>(statement_type, properties, types, names, context->GetClientProperties(),
	                                    gstate.buffered_data);
}

bool PhysicalBufferedCollector::ParallelSink() const {
	return parallel;
}

bool PhysicalBufferedCollector::SinkOrderDependent() const {
	return true;
}

} // namespace duckdb
