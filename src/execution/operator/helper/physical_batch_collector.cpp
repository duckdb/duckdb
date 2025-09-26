#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"

#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/result_set_manager.hpp"

namespace duckdb {

PhysicalBatchCollector::PhysicalBatchCollector(PhysicalPlan &physical_plan, PreparedStatementData &data)
    : PhysicalResultCollector(physical_plan, data) {
}

SinkResultType PhysicalBatchCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<BatchCollectorLocalState>();
	state.data.Append(chunk, state.partition_info.batch_index.GetIndex());
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalBatchCollector::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<BatchCollectorGlobalState>();
	auto &state = input.local_state.Cast<BatchCollectorLocalState>();

	lock_guard<mutex> lock(gstate.glock);
	gstate.data.Merge(state.data);

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<BatchCollectorGlobalState>();
	auto collection = gstate.data.FetchCollection();
	D_ASSERT(collection);
	auto result_set = ResultSetManager::Get(context).Add(std::move(collection), properties.memory_management_type);
	auto result = make_uniq<MaterializedQueryResult>(statement_type, properties, names, std::move(result_set),
	                                                 context.GetClientProperties());
	gstate.result = std::move(result);
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalBatchCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BatchCollectorLocalState>(context.client, *this);
}

unique_ptr<GlobalSinkState> PhysicalBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BatchCollectorGlobalState>(context, *this);
}

unique_ptr<QueryResult> PhysicalBatchCollector::GetResult(GlobalSinkState &state) const {
	auto &gstate = state.Cast<BatchCollectorGlobalState>();
	D_ASSERT(gstate.result);
	return std::move(gstate.result);
}

} // namespace duckdb
