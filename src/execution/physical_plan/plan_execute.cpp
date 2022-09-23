#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExecute &op) {
	if (!op.prepared->plan) {
		D_ASSERT(op.children.size() == 1);
		auto owned_plan = CreatePlan(*op.children[0]);
		auto execute = make_unique<PhysicalExecute>(owned_plan.get());
		execute->owned_plan = move(owned_plan);
		execute->prepared = move(op.prepared);
		return move(execute);
	} else {
		D_ASSERT(op.children.size() == 0);
		return make_unique<PhysicalExecute>(op.prepared->plan.get());
	}
}

} // namespace duckdb
