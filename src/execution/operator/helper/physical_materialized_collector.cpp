#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"

#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PhysicalMaterializedCollector::PhysicalMaterializedCollector(PhysicalPlan &physical_plan, PreparedStatementData &data,
                                                             bool parallel)
    : PhysicalResultCollector(physical_plan, data), parallel(parallel) {
}

class MaterializedCollectorGlobalState : public GlobalSinkState {
public:
	mutex glock;
	unique_ptr<ColumnDataCollection> collection;
	shared_ptr<ClientContext> context;
};

class MaterializedCollectorLocalState : public LocalSinkState {
public:
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
};

SinkResultType PhysicalMaterializedCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<MaterializedCollectorLocalState>();
	lstate.collection->Append(lstate.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalMaterializedCollector::Combine(ExecutionContext &context,
                                                             OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<MaterializedCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<MaterializedCollectorLocalState>();
	if (lstate.collection->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}

	lock_guard<mutex> l(gstate.glock);
	if (!gstate.collection) {
		gstate.collection = std::move(lstate.collection);
	} else {
		gstate.collection->Combine(*lstate.collection);
	}

	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalMaterializedCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<MaterializedCollectorGlobalState>();
	state->context = context.shared_from_this();
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalMaterializedCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<MaterializedCollectorLocalState>();
	// Use the DatabaseInstance BufferManager because the query result can outlive the ClientContext
	auto &buffer_manager = BufferManager::GetBufferManager(*context.client.db);
	state->collection = make_uniq<ColumnDataCollection>(buffer_manager, types);
	state->collection->InitializeAppend(state->append_state);
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalMaterializedCollector::GetResult(GlobalSinkState &state) const {
	auto &gstate = state.Cast<MaterializedCollectorGlobalState>();
	if (!gstate.collection) {
		// Use the DatabaseInstance BufferManager because the query result can outlive the ClientContext
		auto &buffer_manager = BufferManager::GetBufferManager(*gstate.context->db);
		gstate.collection = make_uniq<ColumnDataCollection>(buffer_manager, types);
	}
	auto managed_result = QueryResultManager::Get(*gstate.context).Add(std::move(gstate.collection));
	auto result = make_uniq<MaterializedQueryResult>(statement_type, properties, names, std::move(managed_result),
	                                                 gstate.context->GetClientProperties());
	return std::move(result);
}

bool PhysicalMaterializedCollector::ParallelSink() const {
	return parallel;
}

bool PhysicalMaterializedCollector::SinkOrderDependent() const {
	return true;
}

} // namespace duckdb
