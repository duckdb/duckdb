#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOrder &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
	if (!op.orders.empty()) {
		vector<idx_t> projection_map;
		if (op.HasProjectionMap()) {
			projection_map = std::move(op.projection_map);
		} else {
			for (idx_t i = 0; i < plan->types.size(); i++) {
				projection_map.push_back(i);
			}
		}
		auto order = make_uniq<PhysicalOrder>(op.types, std::move(op.orders), std::move(projection_map),
		                                      op.estimated_cardinality);
		order->children.push_back(std::move(plan));
		plan = std::move(order);
	}
	return plan;
}

} // namespace duckdb
