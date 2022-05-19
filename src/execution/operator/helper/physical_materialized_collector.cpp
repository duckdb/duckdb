#include "duckdb/execution/operator/helper/physical_materialized_collector.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {

PhysicalMaterializedCollector::PhysicalMaterializedCollector(PhysicalOperator *plan, vector<string> names,
                                                             vector<LogicalType> types, bool parallel)
    : PhysicalResultCollector(plan, move(names), move(types)), parallel(parallel) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class MaterializedCollectorGlobalState : public GlobalSinkState {
public:
	mutex glock;
	unique_ptr<MaterializedQueryResult> result;
};

SinkResultType PhysicalMaterializedCollector::Sink(ExecutionContext &context, GlobalSinkState &gstate_p,
                                                   LocalSinkState &lstate, DataChunk &input) const {
	auto &gstate = (MaterializedCollectorGlobalState &)gstate_p;
	lock_guard<mutex> lock(gstate.glock);
	gstate.result->collection.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> PhysicalMaterializedCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<MaterializedCollectorGlobalState>();
	state->result = make_unique<MaterializedQueryResult>(StatementType::INVALID_STATEMENT, types, names);
	return move(state);
}

unique_ptr<QueryResult> PhysicalMaterializedCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = (MaterializedCollectorGlobalState &)state;
	D_ASSERT(gstate.result);
	return move(gstate.result);
}

bool PhysicalMaterializedCollector::ParallelSink() const {
	return parallel;
}

} // namespace duckdb
