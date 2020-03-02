#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalAnyJoin &op) {
	// first visit the child nodes
	assert(op.children.size() == 2);
	assert(op.condition);

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);

	// create the blockwise NL join
	return make_unique<PhysicalBlockwiseNLJoin>(op, move(left), move(right), move(op.condition), op.join_type);
}
