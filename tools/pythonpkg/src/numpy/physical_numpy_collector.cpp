#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb_python/numpy/physical_numpy_collector.hpp"
#include "duckdb_python/numpy/numpy_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb_python/numpy/physical_numpy_batch_collector.hpp"
#include "duckdb_python/numpy/numpy_merge_event.hpp"

namespace duckdb {

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
		return make_uniq_base<PhysicalResultCollector, PhysicalNumpyCollector>(data, true);
	} else if (!PhysicalPlanGenerator::UseBatchIndex(context, *data.plan)) {
		// the plan is order preserving, but we cannot use the batch index: use a single-threaded result collector
		return make_uniq_base<PhysicalResultCollector, PhysicalNumpyCollector>(data, false);
	} else {
		// we care about maintaining insertion order and the sources all support batch indexes
		// use a batch collector
		return make_uniq_base<PhysicalResultCollector, PhysicalNumpyBatchCollector>(data);
	}
}

SinkCombineResultType PhysicalNumpyCollector::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<NumpyCollectorGlobalState>();
	auto &lstate = input.local_state.Cast<MaterializedCollectorLocalState>();
	if (lstate.collection->Count() == 0) {
		py::gil_scoped_acquire gil;
		lstate.collection.reset();
		return SinkCombineResultType::FINISHED;
	}

	// Collect all the collections
	lock_guard<mutex> l(gstate.glock);
	gstate.batches[gstate.batch_index++] = std::move(lstate.collection);
	return SinkCombineResultType::FINISHED;
}

unique_ptr<QueryResult> PhysicalNumpyCollector::GetResult(GlobalSinkState &state_p) {
	auto &gstate = state_p.Cast<NumpyCollectorGlobalState>();
	return std::move(gstate.result);
}

unique_ptr<GlobalSinkState> PhysicalNumpyCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<NumpyCollectorGlobalState>();
}

SinkFinalizeType PhysicalNumpyCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<NumpyCollectorGlobalState>();
	D_ASSERT(gstate.collection == nullptr);

	gstate.collection = make_uniq<BatchedDataCollection>(context, types, std::move(gstate.batches), true);

	// Pre-allocate the conversion result
	unique_ptr<NumpyResultConversion> result;
	auto total_tuple_count = gstate.collection->Count();
	auto &types = gstate.collection->Types();
	{
		py::gil_scoped_acquire gil;
		result = make_uniq<NumpyResultConversion>(types, total_tuple_count, context.GetClientProperties());
		result->SetCardinality(total_tuple_count);
	}
	if (total_tuple_count == 0) {
		// Create the result containing a single empty numpy result
		gstate.result = make_uniq<NumpyQueryResult>(statement_type, properties, names, std::move(result),
		                                            context.GetClientProperties());
		return SinkFinalizeType::READY;
	}

	// Spawn an event that will populate the conversion result
	auto new_event = make_shared<NumpyMergeEvent>(*result, *gstate.collection, pipeline);
	event.InsertEvent(std::move(new_event));

	// Already create the final query result
	gstate.result = make_uniq<NumpyQueryResult>(statement_type, properties, names, std::move(result),
	                                            context.GetClientProperties());

	return SinkFinalizeType::READY;
}

} // namespace duckdb
