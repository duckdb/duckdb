#include "duckdb_python/numpy/physical_numpy_collector.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb_python/numpy/numpy_query_result.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"

namespace duckdb {

PhysicalNumpyCollector::PhysicalNumpyCollector(PreparedStatementData &data, bool parallel)
    : PhysicalResultCollector(data), parallel(parallel) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class NumpyCollectorGlobalState : public GlobalSinkState {
public:
	mutex glock;
	unique_ptr<NumpyResultConversion> collection;
	shared_ptr<ClientContext> context;
};

class NumpyCollectorLocalState : public LocalSinkState {
public:
	unique_ptr<NumpyResultConversion> collection;
};

SinkResultType PhysicalNumpyCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<NumpyCollectorLocalState>();
	lstate.collection->Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalNumpyCollector::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                     LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<NumpyCollectorGlobalState>();
	auto &lstate = lstate_p.Cast<NumpyCollectorLocalState>();
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

unique_ptr<GlobalSinkState> PhysicalNumpyCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<NumpyCollectorGlobalState>();
	state->context = context.shared_from_this();
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalNumpyCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<NumpyCollectorLocalState>();
	state->collection = make_uniq<NumpyResultConversion>(types, STANDARD_VECTOR_SIZE);
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalNumpyCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = state.Cast<NumpyCollectorGlobalState>();
	if (!gstate.collection) {
		gstate.collection = make_uniq<NumpyResultConversion>(types, 0);
	}
	auto result = make_uniq<NumpyQueryResult>(statement_type, properties, names, std::move(gstate.collection),
	                                          gstate.context->GetClientProperties());
	return std::move(result);
}

bool PhysicalNumpyCollector::ParallelSink() const {
	return parallel;
}

bool PhysicalNumpyCollector::SinkOrderDependent() const {
	return true;
}

} // namespace duckdb
