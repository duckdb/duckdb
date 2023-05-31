#include "duckdb_python/numpy/physical_numpy_batch_collector.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb_python/numpy/numpy_query_result.hpp"
#include "duckdb_python/numpy/numpy_merge_event.hpp"

namespace duckdb {

SinkFinalizeType PhysicalNumpyBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                       GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<BatchCollectorGlobalState>();

	// Pre-allocate the conversion result
	unique_ptr<NumpyResultConversion> result;
	auto total_tuple_count = gstate.data.Count();
	auto &types = gstate.data.Types();
	{
		py::gil_scoped_acquire gil;
		result = make_uniq<NumpyResultConversion>(types, total_tuple_count);
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
