#include "duckdb_python/numpy/physical_numpy_collector.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb_python/numpy/numpy_query_result.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb_python/numpy/physical_numpy_batch_collector.hpp"

namespace duckdb {

PhysicalNumpyCollector::PhysicalNumpyCollector(PreparedStatementData &data, bool parallel)
    : PhysicalResultCollector(data), parallel(parallel) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class NumpyCollectorGlobalState : public GlobalSinkState {
public:
	~NumpyCollectorGlobalState() override {
		py::gil_scoped_acquire gil;
		collections.clear();
	}

public:
	mutex glock;
	vector<unique_ptr<NumpyResultConversion>> collections;
	shared_ptr<ClientContext> context;
};

class NumpyCollectorLocalState : public LocalSinkState {
public:
	~NumpyCollectorLocalState() override {
		// If an exception occurred, this is destroyed without the GIL held
		if (py::gil_check()) {
			collection.reset();
		} else {
			py::gil_scoped_acquire gil;
			collection.reset();
		}
	}

public:
	unique_ptr<NumpyResultConversion> collection;
};

unique_ptr<PhysicalResultCollector> PhysicalNumpyCollector::Create(ClientContext &context,
                                                                   PreparedStatementData &data) {
	(void)context;
	// The creation of `py::array` requires this module, and when this is imported for the first time from a thread that
	// is not the main execution thread this might cause a crash. So we import it here while we're still in the main
	// thread.
	{
		py::gil_scoped_acquire gil;
		auto numpy_internal = py::module_::import("numpy.core.multiarray");
	}

	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, *data.plan)) {
		// the plan is not order preserving, so we just use the parallel materialized collector
		return make_uniq_base<PhysicalResultCollector, PhysicalNumpyCollector>(data, false);
	} else if (!PhysicalPlanGenerator::UseBatchIndex(context, *data.plan)) {
		// the plan is order preserving, but we cannot use the batch index: use a single-threaded result collector
		return make_uniq_base<PhysicalResultCollector, PhysicalNumpyCollector>(data, false);
	} else {
		// we care about maintaining insertion order and the sources all support batch indexes
		// use a batch collector
		return make_uniq_base<PhysicalResultCollector, PhysicalNumpyBatchCollector>(data);
	}
}

SinkResultType PhysicalNumpyCollector::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<NumpyCollectorLocalState>();
	py::gil_scoped_acquire gil;
	lstate.collection->Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalNumpyCollector::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                     LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<NumpyCollectorGlobalState>();
	auto &lstate = lstate_p.Cast<NumpyCollectorLocalState>();

	lock_guard<mutex> l(gstate.glock);
	gstate.collections.push_back(std::move(lstate.collection));
}

SinkFinalizeType PhysicalNumpyCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  GlobalSinkState &gstate_p) const {
	// auto &gstate = gstate_p.Cast<NumpyCollectorGlobalState>();

	// auto new_event = make_shared<HashAggregateMergeEvent>(*this, gstate, &pipeline);
	// event.InsertEvent(std::move(new_event));

	return SinkFinalizeType::READY;
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

	unique_ptr<NumpyResultConversion> collection;
	D_ASSERT(!gstate.collections.empty());
	if (gstate.collections.size() == 1) {
		collection = std::move(gstate.collections[0]);
	} else {
		py::gil_scoped_acquire gil;
		collection = make_uniq<NumpyResultConversion>(std::move(gstate.collections), types);
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
