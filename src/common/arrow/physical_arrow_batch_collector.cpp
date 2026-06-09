#include "duckdb/common/arrow/physical_arrow_batch_collector.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/arrow/arrow_query_result.hpp"
#include "duckdb/common/arrow/arrow_merge_event.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/arrow/physical_arrow_collector.hpp"

namespace duckdb {

unique_ptr<GlobalSinkState> PhysicalArrowBatchCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ArrowBatchGlobalState>(context, *this);
}

SinkFinalizeType PhysicalArrowBatchCollector::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                       OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ArrowBatchGlobalState>();

	auto total_tuple_count = gstate.data.Count();
	if (total_tuple_count == 0) {
		// Create the result containing a single empty result conversion
		auto result = make_uniq<ArrowQueryResult>(statement_type, properties, names, types,
		                                          context.GetClientProperties(), record_batch_size);
		result->BuildCachedSchema();
		gstate.result = std::move(result);
		return SinkFinalizeType::READY;
	}

	// Already create the final query result
	auto result = make_uniq<ArrowQueryResult>(statement_type, properties, names, types, context.GetClientProperties(),
	                                          record_batch_size);
	// Cache the schema while the producing transaction is still active (see duckdb/duckdb-python#475).
	result->BuildCachedSchema();
	auto &arrow_result = *result;
	gstate.result = std::move(result);
	// Spawn an event that will populate the conversion result
	auto new_event = make_shared_ptr<ArrowMergeEvent>(arrow_result, gstate.data, pipeline);
	event.InsertEvent(std::move(new_event));

	return SinkFinalizeType::READY;
}

} // namespace duckdb
