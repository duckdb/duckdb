#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalOrder &op) {
	D_ASSERT(op.children.size() == 1);

	auto &plan = CreatePlan(*op.children[0]);
	if (op.orders.empty()) {
		return plan;
	}

	auto projection_map = PhysicalJoin::FillProjectionMap(plan, op.projection_map);
	auto &order =
	    Make<PhysicalOrder>(op.types, std::move(op.orders), std::move(projection_map), op.estimated_cardinality);
	order.children.push_back(plan);
	return order;
}

} // namespace duckdb
