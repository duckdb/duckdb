#include "duckdb/optimizer/compressed_materialization_optimizer.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

void CompressedMaterialization::CompressAggregate(unique_ptr<LogicalOperator> *op_ptr) {
	auto &aggregate = (LogicalAggregate &)**op_ptr;

	// No need to compress if there are no groups
	if (aggregate.groups.empty()) {
		return;
	}
}

} // namespace duckdb
