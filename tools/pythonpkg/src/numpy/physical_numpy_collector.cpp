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
	vector<unique_ptr<NumpyResultConversion>> collections;
	shared_ptr<ClientContext> context;
};

class NumpyCollectorLocalState : public LocalSinkState {
public:
	unique_ptr<NumpyResultConversion> collection;
};

unique_ptr<PhysicalResultCollector> PhysicalNumpyCollector::Create(ClientContext &context,
                                                                   PreparedStatementData &data) {
	(void)context;
	// Always create a parallel result collector
	return make_uniq_base<PhysicalResultCollector, PhysicalNumpyCollector>(data, true);
}

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
	gstate.collections.push_back(std::move(lstate.collection));
}

unique_ptr<GlobalSinkState> PhysicalNumpyCollector::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<NumpyCollectorGlobalState>();
	state->context = context.shared_from_this();
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalNumpyCollector::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<NumpyCollectorLocalState>();
	{
		py::gil_scoped_acquire gil;
		state->collection = make_uniq<NumpyResultConversion>(types, STANDARD_VECTOR_SIZE);
	}
	return std::move(state);
}

unique_ptr<QueryResult> PhysicalNumpyCollector::GetResult(GlobalSinkState &state) {
	auto &gstate = state.Cast<NumpyCollectorGlobalState>();
	idx_t result_size = 0;
	for (auto &collection : gstate.collections) {
		result_size += collection->Count();
	}
	unique_ptr<NumpyResultConversion> collection;
	{
		py::gil_scoped_acquire gil;
		collection = make_uniq<NumpyResultConversion>(types, result_size);
		if (result_size != 0) {
			collection->Merge(gstate.collections);
		}
	}
	auto result = make_uniq<NumpyQueryResult>(statement_type, properties, names, std::move(collection),
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
