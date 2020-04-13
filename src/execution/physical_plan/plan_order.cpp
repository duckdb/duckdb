#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOrder &op) {
	assert(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
	if (op.orders.size() > 0) {
		auto order = make_unique<PhysicalOrder>(op.types, move(op.orders));
		order->children.push_back(move(plan));
		plan = move(order);
	}
	return plan;
}
