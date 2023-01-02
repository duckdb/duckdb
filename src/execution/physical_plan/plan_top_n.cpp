#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalTopN &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	auto top_n =
	    make_unique<PhysicalTopN>(op.types, std::move(op.orders), (idx_t)op.limit, op.offset, op.estimated_cardinality);
	top_n->children.push_back(std::move(plan));
	return std::move(top_n);
}

} // namespace duckdb
