#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalBatchCollector::PhysicalBatchCollector(PreparedStatementData &data) : PhysicalResultCollector(data) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BatchCollectorGlobalState : public GlobalSinkState {
public:
	BatchCollectorGlobalState(ClientContext &context, const PhysicalBatchCollector &op) : data(op.types) {
	}

	mutex glock;
	BatchedDataCollection data;
	unique_ptr<MaterializedQueryResult> result;
};

class BatchCollectorLocalState : public LocalSinkState {
public:
	BatchCollectorLocalState(ClientContext &context, const PhysicalBatchCollector &op) : data(op.types) {
	}

	BatchedDataCollection data;
};

SinkResultType PhysicalBatchCollector::Sink(ExecutionContext &context, GlobalSinkState &gstate,
                                            LocalSinkState &lstate_p, DataChunk &input) const {
	auto &state = (BatchCollectorLocalState &)lstate_p;
	state.data.Append(input, state.batch_index);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalBatchCollector::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                     LocalSinkState &lstate_p) const {
	auto &gstate = (BatchCollectorGlobalState &)gstate_p;
	auto &state = (BatchCollectorLocalState &)lstate_p;

	lock_guard<mutex> lock(gstate.glock);
	gstate.data.Merge(state.data);
}

SinkFinalizeType PhysicalBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  GlobalSinkState &gstate_p) const {
	auto &gstate = (BatchCollectorGlobalState &)gstate_p;
	auto collection = gstate.data.FetchCollection();
	D_ASSERT(collection);
	auto result = make_unique<MaterializedQueryResult>(statement_type, properties, names, move(collection),
	                                                   context.GetClientProperties());
	gstate.result = move(result);
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalBatchCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<BatchCollectorLocalState>(context.client, *this);
}

unique_ptr<GlobalSinkState> PhysicalBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<BatchCollectorGlobalState>(context, *this);
}

unique_ptr<QueryResult> PhysicalBatchCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = (BatchCollectorGlobalState &)state;
	D_ASSERT(gstate.result);
	return move(gstate.result);
}

} // namespace duckdb
