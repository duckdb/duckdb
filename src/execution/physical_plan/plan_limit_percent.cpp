#include "duckdb/execution/operator/helper/physical_limit_percent.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_limit_percent.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalLimitPercent &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	auto limit = make_uniq<PhysicalLimitPercent>(op.types, op.limit_percent, op.offset_val, std::move(op.limit),
	                                             std::move(op.offset), op.estimated_cardinality);
	limit->children.push_back(std::move(plan));
	return std::move(limit);
}

} // namespace duckdb
