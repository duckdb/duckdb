#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

PhysicalMaterializedCollector::PhysicalMaterializedCollector(PreparedStatementData &data, bool parallel)
    : PhysicalResultCollector(data), parallel(parallel) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
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

SinkResultType PhysicalMaterializedCollector::Sink(ExecutionContext &context, GlobalSinkState &gstate_p,
                                                   LocalSinkState &lstate_p, DataChunk &input) const {
	auto &lstate = lstate_p.Cast<MaterializedCollectorLocalState>();
	lstate.collection->Append(lstate.append_state, input);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalMaterializedCollector::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                            LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<MaterializedCollectorGlobalState>();
	auto &lstate = lstate_p.Cast<MaterializedCollectorLocalState>();
	if (lstate.collection->Count() == 0) {
		return;
	}

	lock_guard<mutex> l(gstate.glock);
	if (!gstate.collection) {
		gstate.collection = std::move(lstate.collection);
	} else {
		gstate.collection->Combine(*lstate.collection);
	}
}

unique_ptr<GlobalSinkState> PhysicalMaterializedCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<MaterializedCollectorGlobalState>();
	state->context = context.shared_from_this();
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalMaterializedCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<MaterializedCollectorLocalState>();
	state->collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	state->collection->InitializeAppend(state->append_state);
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalMaterializedCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = state.Cast<MaterializedCollectorGlobalState>();
	if (!gstate.collection) {
		gstate.collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}
	auto result = make_uniq<MaterializedQueryResult>(statement_type, properties, names, std::move(gstate.collection),
	                                                 gstate.context->GetClientProperties());
	return std::move(result);
}

bool PhysicalMaterializedCollector::ParallelSink() const {
	return parallel;
}

} // namespace duckdb
