#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalExecute &op) {
	if (op.prepared->physical_plan) {
		D_ASSERT(op.children.empty());
		return Make<PhysicalExecute>(op.prepared->physical_plan->Root());
	}

	D_ASSERT(op.children.size() == 1);
	auto &plan = CreatePlan(*op.children[0]);
	auto &execute = Make<PhysicalExecute>(plan);
	auto &cast_execute = execute.Cast<PhysicalExecute>();
	cast_execute.prepared = op.prepared;
	return execute;
}

} // namespace duckdb
