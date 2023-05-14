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
			projections = std::move(op.projections);
		}
		auto order =
		    make_uniq<PhysicalOrder>(op.types, std::move(op.orders), std::move(projections), op.estimated_cardinality);
		order->children.push_back(std::move(plan));
		plan = std::move(order);
	}
	return plan;
}

} // namespace duckdb
