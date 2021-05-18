#include "duckdb/execution/base_aggregate_hashtable.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

vector<AggregateObject> AggregateObject::CreateAggregateObjects(const vector<BoundAggregateExpression *> &bindings) {
	vector<AggregateObject> aggregates;
	for (auto &binding : bindings) {
		auto payload_size = binding->function.state_size();
#ifndef DUCKDB_ALLOW_UNDEFINED
		payload_size = BaseAggregateHashTable::Align(payload_size);
#endif
		aggregates.emplace_back(binding->function, binding->bind_info.get(), binding->children.size(), payload_size,
		                        binding->distinct, binding->return_type.InternalType(), binding->filter.get());
	}
	return aggregates;
}

BaseAggregateHashTable::BaseAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types_p,
                                               vector<LogicalType> payload_types_p,
                                               vector<AggregateObject> aggregate_objects)
    : buffer_manager(buffer_manager), aggregates(move(aggregate_objects)), group_types(move(group_types_p)),
      payload_types(move(payload_types_p)), group_width(0), group_mask_width(0), group_padding(0), payload_width(0) {

	for (idx_t i = 0; i < group_types.size(); i++) {
		group_width += GetTypeIdSize(group_types[i].InternalType());
	}

	for (idx_t i = 0; i < aggregates.size(); i++) {
		payload_width += aggregates[i].payload_size;
#ifndef DUCKDB_ALLOW_UNDEFINED
		D_ASSERT(aggregates[i].payload_size == BaseAggregateHashTable::Align(aggregates[i].payload_size));
#endif
	}

	empty_payload_data = unique_ptr<data_t[]>(new data_t[payload_width]);
	// initialize the aggregates to the NULL value
	auto pointer = empty_payload_data.get();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		aggr.function.initialize(pointer);
		pointer += aggr.payload_size;
	}

	D_ASSERT(group_width > 0);

#ifndef DUCKDB_ALLOW_UNDEFINED
	auto aligned_group_width = BaseAggregateHashTable::Align(group_width);
	group_padding = aligned_group_width - group_width;
	group_width += group_padding;
#endif
}

void BaseAggregateHashTable::CallDestructors(Vector &state_vector, idx_t count) {
	if (count == 0) {
		return;
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		if (aggr.function.destructor) {
			aggr.function.destructor(state_vector, count);
		}
		// move to the next aggregate state
		VectorOperations::AddInPlace(state_vector, aggr.payload_size, count);
	}
}

} // namespace duckdb
