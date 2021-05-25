#include "duckdb/execution/base_aggregate_hashtable.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

BaseAggregateHashTable::BaseAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types_p,
                                               vector<LogicalType> payload_types_p,
                                               vector<AggregateObject> aggregate_objects)
    : buffer_manager(buffer_manager), payload_types(move(payload_types_p)) {

	layout.Initialize(move(group_types_p), move(aggregate_objects));

	empty_payload_data = unique_ptr<data_t[]>(new data_t[layout.GetAggrWidth()]);
	// initialize the aggregates to the NULL value
	auto pointer = empty_payload_data.get();
	for (auto &aggr : layout.GetAggregates()) {
		aggr.function.initialize(pointer);
		pointer += aggr.payload_size;
	}
}

void BaseAggregateHashTable::CallDestructors(Vector &state_vector, idx_t count) {
	if (count == 0) {
		return;
	}
	for (auto &aggr : layout.GetAggregates()) {
		if (aggr.function.destructor) {
			aggr.function.destructor(state_vector, count);
		}
		// move to the next aggregate state
		VectorOperations::AddInPlace(state_vector, aggr.payload_size, count);
	}
}

} // namespace duckdb
