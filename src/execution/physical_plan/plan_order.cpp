#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOrder &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
	if (!op.orders.empty()) {
		vector<idx_t> projections;
		if (op.projections.empty()) {
			for (idx_t i = 0; i < plan->types.size(); i++) {
				projections.push_back(i);
			}
		} else {
			projections = move(op.projections);
		}
		auto order = make_unique<PhysicalOrder>(op.types, move(op.orders), move(projections), op.estimated_cardinality);
		order->children.push_back(move(plan));
		plan = move(order);
	}
	return plan;
}

} // namespace duckdb
