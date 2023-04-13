#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

void CompressedMaterialization::CompressAggregate(unique_ptr<LogicalOperator> &op) {
	auto &aggregate = (LogicalAggregate &)*op;

	// No need to compress if there are no groups
	if (aggregate.groups.empty()) {
		return;
	}
}

} // namespace duckdb
