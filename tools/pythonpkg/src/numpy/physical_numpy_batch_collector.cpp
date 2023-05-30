#include "duckdb_python/numpy/physical_numpy_batch_collector.hpp"
#include "duckdb_python/numpy/batched_numpy_conversion.hpp"
#include "duckdb_python/numpy/numpy_query_result.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalNumpyBatchCollector::PhysicalNumpyBatchCollector(PreparedStatementData &data) : PhysicalResultCollector(data) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BatchNumpyCollectorGlobalState : public GlobalSinkState {
public:
	BatchNumpyCollectorGlobalState(ClientContext &context, const PhysicalNumpyBatchCollector &op) : data(op.types) {
	}

	mutex glock;
	BatchedNumpyConversion data;
	unique_ptr<NumpyQueryResult> result;
};

class BatchNumpyCollectorLocalState : public LocalSinkState {
public:
	BatchNumpyCollectorLocalState(ClientContext &context, const PhysicalNumpyBatchCollector &op) : data(op.types) {
	}

	BatchedNumpyConversion data;
};

SinkResultType PhysicalNumpyBatchCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<BatchNumpyCollectorLocalState>();
	state.data.Append(chunk, state.partition_info.batch_index.GetIndex());
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalNumpyBatchCollector::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                          LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<BatchNumpyCollectorGlobalState>();
	auto &state = lstate_p.Cast<BatchNumpyCollectorLocalState>();

	lock_guard<mutex> lock(gstate.glock);
	gstate.data.Merge(state.data);
}

SinkFinalizeType PhysicalNumpyBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                       GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<BatchNumpyCollectorGlobalState>();
	auto collection = gstate.data.FetchCollection();
	D_ASSERT(collection);
	auto result = make_uniq<NumpyQueryResult>(statement_type, properties, names, std::move(collection),
	                                          context.GetClientProperties());
	gstate.result = std::move(result);
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalNumpyBatchCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BatchNumpyCollectorLocalState>(context.client, *this);
}

unique_ptr<GlobalSinkState> PhysicalNumpyBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BatchNumpyCollectorGlobalState>(context, *this);
}

unique_ptr<QueryResult> PhysicalNumpyBatchCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = state.Cast<BatchNumpyCollectorGlobalState>();
	D_ASSERT(gstate.result);
	return std::move(gstate.result);
}

} // namespace duckdb
