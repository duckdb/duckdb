#include "duckdb/execution/operator/helper/physical_prune_columns.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_prune_columns.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPruneColumns &op) {
	assert(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
	if (plan->GetTypes().size() > op.column_limit) {
		// only prune if we need to
		auto node = make_unique<PhysicalPruneColumns>(op, op.column_limit);
		node->children.push_back(move(plan));
		plan = move(node);
	}
	return plan;
}
