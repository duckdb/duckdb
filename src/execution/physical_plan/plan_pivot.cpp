#include "duckdb/execution/operator/projection/physical_pivot.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPivot &op) {
	D_ASSERT(op.children.size() == 1);
	auto child_plan = CreatePlan(*op.children[0]);
	auto pivot = make_uniq<PhysicalPivot>(std::move(op.types), std::move(child_plan), std::move(op.bound_pivot));
	return std::move(pivot);
}

} // namespace duckdb
