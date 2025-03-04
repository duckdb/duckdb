#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalOrder &op) {
	D_ASSERT(op.children.size() == 1);

	auto &plan_ref = CreatePlan(*op.children[0]);
	if (op.orders.empty()) {
		return plan_ref;
	}

	vector<idx_t> projection_map;
	if (op.HasProjectionMap()) {
		projection_map = std::move(op.projection_map);
	} else {
		for (idx_t i = 0; i < plan_ref.types.size(); i++) {
			projection_map.push_back(i);
		}
	}
	auto &order_ref =
	    Make<PhysicalOrder>(op.types, std::move(op.orders), std::move(projection_map), op.estimated_cardinality);
	order_ref.children.push_back(plan_ref);
	return order_ref;
}

} // namespace duckdb
