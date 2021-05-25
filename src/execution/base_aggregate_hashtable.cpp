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

RowLayout::RowLayout() : flag_width(0), data_width(0), aggr_width(0), row_width(0) {
}

void RowLayout::Initialize(vector<LogicalType> types_p, Aggregates aggregates_p) {
	offsets.clear();
	types = types_p;

	// Null mask at the front - 1 bit per value.
	flag_width = ValidityBytes::ValidityMaskSize(types.size());
	row_width = flag_width;

	// Data columns. No alignment required.
	for (const auto &type : types) {
		offsets.push_back(row_width);
		row_width += GetTypeIdSize(type.InternalType());
	}

	// Alignment padding for aggregates
#ifndef DUCKDB_ALLOW_UNDEFINED
	row_width = Align(row_width);
#endif
	data_width = row_width - flag_width;

	// Aggregate fields.
	aggregates = move(aggregates_p);
	for (auto &aggregate : aggregates) {
		offsets.push_back(row_width);
		row_width += aggregate.payload_size;
#ifndef DUCKDB_ALLOW_UNDEFINED
		D_ASSERT(aggregate.payload_size == Align(aggregate.payload_size));
#endif
	}
	aggr_width = row_width - data_width - flag_width;

	// Alignment padding for the next row
#ifndef DUCKDB_ALLOW_UNDEFINED
	row_width = Align(row_width);
#endif
}

void RowLayout::Initialize(vector<LogicalType> types_p) {
	Initialize(move(types_p), Aggregates());
}

void RowLayout::Initialize(Aggregates aggregates_p) {
	Initialize(vector<LogicalType>(), move(aggregates_p));
}

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
