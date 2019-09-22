#include "execution/operator/order/physical_order_limit.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_order_limit.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOrderAndLimit &op) {
	assert(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	auto order_and_limit = make_unique<PhysicalOrderAndLimit>(op, move(op.orders), op.limit, op.offset);
	order_and_limit->children.push_back(move(plan));
	return move(order_and_limit);
}
