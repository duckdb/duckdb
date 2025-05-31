#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalTopN &op) {
	D_ASSERT(op.children.size() == 1);
	auto &plan = CreatePlan(*op.children[0]);
	return Make<PhysicalTopN>(plan, op.types, std::move(op.orders), NumericCast<idx_t>(op.limit),
	                          NumericCast<idx_t>(op.offset), std::move(op.dynamic_filter), op.estimated_cardinality);
}

} // namespace duckdb
