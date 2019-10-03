#include "execution/operator/order/physical_top_n.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_top_n.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalTopN &op) {
	assert(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	auto top_n = make_unique<PhysicalTopN>(op, move(op.orders), op.limit, op.offset);
	top_n->children.push_back(move(plan));
	return move(top_n);
}
