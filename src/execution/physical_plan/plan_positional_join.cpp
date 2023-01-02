#include "duckdb/execution/operator/join/physical_pos_join.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_pos_join.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPositionalJoin &op) {
	D_ASSERT(op.children.size() == 2);

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);
	return make_unique<PhysicalPositionalJoin>(op.types, move(left), move(right), op.estimated_cardinality);
}

} // namespace duckdb
