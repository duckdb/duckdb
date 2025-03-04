#include "duckdb/execution/operator/projection/physical_pivot.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalPivot &op) {
	D_ASSERT(op.children.size() == 1);
	auto &child_ref = CreatePlan(*op.children[0]);
	return Make<PhysicalPivot>(std::move(op.types), child_ref, std::move(op.bound_pivot));
}

} // namespace duckdb
