#include "duckdb/execution/base_aggregate_hashtable.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

BaseAggregateHashTable::BaseAggregateHashTable(Allocator &allocator, const vector<AggregateObject> &aggregates,
                                               BufferManager &buffer_manager, vector<LogicalType> payload_types_p)
    : allocator(allocator), buffer_manager(buffer_manager), payload_types(move(payload_types_p)) {
	filter_set.Initialize(allocator, aggregates, payload_types);
}

} // namespace duckdb
