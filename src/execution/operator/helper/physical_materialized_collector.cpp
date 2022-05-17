#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/common/types/batched_chunk_collection.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {

PhysicalMaterializedCollector::PhysicalMaterializedCollector(PhysicalOperator *plan) :
      PhysicalResultCollector(plan) {}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class MaterializedCollectorGlobalState : public GlobalSinkState {
public:
	mutex glock;
	BatchedChunkCollection data;
	unique_ptr<MaterializedQueryResult> result;
};

class MaterializedCollectorLocalState : public LocalSinkState {
public:
	BatchedChunkCollection data;
};

SinkResultType PhysicalMaterializedCollector::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate_p,
					DataChunk &input) const {
	auto &state = (MaterializedCollectorLocalState &)lstate_p;
	state.data.Append(input, state.batch_index);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalMaterializedCollector::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (MaterializedCollectorGlobalState &)gstate_p;
	auto &state = (MaterializedCollectorLocalState &)lstate_p;

	lock_guard<mutex> lock(gstate.glock);
	gstate.data.Merge(state.data);
}

SinkFinalizeType PhysicalMaterializedCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
						  GlobalSinkState &gstate_p) const {
	throw InternalException("finalize");
//	auto &gstate = (MaterializedCollectorGlobalState &)gstate_p;
//	auto result = make_unique<MaterializedQueryResult>(StatementType::)
}

unique_ptr<LocalSinkState> PhysicalMaterializedCollector::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<MaterializedCollectorLocalState>();
}

unique_ptr<GlobalSinkState> PhysicalMaterializedCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<MaterializedCollectorGlobalState>();
}

unique_ptr<QueryResult> PhysicalMaterializedCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = (MaterializedCollectorGlobalState &) state;
	D_ASSERT(gstate.result);
	return move(gstate.result);
}

}
