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

} // namespace duckdb
