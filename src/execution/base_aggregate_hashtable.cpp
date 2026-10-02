#include "duckdb/execution/base_aggregate_hashtable.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BaseAggregateHashTable::BaseAggregateHashTable(ClientContext &context, Allocator &allocator,
                                               const vector<AggregateObject> &aggregates,
                                               vector<LogicalType> payload_types_p,
                                               shared_ptr<const AggregateInputLayout> input_layout_p)
    : allocator(allocator), buffer_manager(BufferManager::GetBufferManager(context)),
      layout_ptr(make_shared_ptr<TupleDataLayout>()), payload_types(std::move(payload_types_p)),
      input_layout(input_layout_p ? std::move(input_layout_p)
                                  : make_shared_ptr<AggregateInputLayout>(payload_types, aggregates)) {
	D_ASSERT(input_layout->Payload().GetTypes() == payload_types);
	filter_set.Initialize(context, aggregates, payload_types, input_layout.get());
}

bool BaseAggregateHashTable::AllAggregatesClustered(const vector<AggregateObject> &aggregates) {
	if (aggregates.empty()) {
		return false;
	}
	for (auto &aggregate : aggregates) {
		if (aggregate.filter || !aggregate.function.GetStateClusterUpdateCallback()) {
			return false;
		}
	}
	return true;
}

idx_t BaseAggregateHashTable::CountAggregatesClustered(const vector<AggregateObject> &aggregates) {
	idx_t count = 0;
	for (auto &aggregate : aggregates) {
		if (!aggregate.filter && aggregate.function.GetStateClusterUpdateCallback()) {
			count++;
		}
	}
	return count;
}

} // namespace duckdb
