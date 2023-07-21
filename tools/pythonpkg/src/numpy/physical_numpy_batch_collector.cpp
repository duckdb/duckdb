#include "duckdb_python/numpy/physical_numpy_batch_collector.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb_python/numpy/numpy_query_result.hpp"
#include "duckdb_python/numpy/numpy_merge_event.hpp"

namespace duckdb {

unique_ptr<GlobalSinkState> PhysicalNumpyBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<NumpyBatchGlobalState>(context, *this);
}

SinkFinalizeType PhysicalNumpyBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                       OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<NumpyBatchGlobalState>();

	// Pre-allocate the conversion result
	unique_ptr<NumpyResultConversion> result;
	auto total_tuple_count = gstate.data.Count();
	auto &types = gstate.data.Types();
	{
		py::gil_scoped_acquire gil;
		result = make_uniq<NumpyResultConversion>(types, total_tuple_count);
		result->SetCardinality(total_tuple_count);
	}
	if (total_tuple_count == 0) {
		// Create the result containing a single empty result conversion
		gstate.result = make_uniq<NumpyQueryResult>(statement_type, properties, names, std::move(result),
		                                            context.GetClientProperties());
		return SinkFinalizeType::READY;
	}

	// Spawn an event that will populate the conversion result
	auto new_event = make_shared<NumpyMergeEvent>(*result, gstate.data, pipeline);
	event.InsertEvent(std::move(new_event));

	// Already create the final query result
	gstate.result = make_uniq<NumpyQueryResult>(statement_type, properties, names, std::move(result),
	                                            context.GetClientProperties());

	return SinkFinalizeType::READY;
}

} // namespace duckdb
