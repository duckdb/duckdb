#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"
#include "duckdb/execution/operator/filter/physical_use_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {
PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalUseBF &op) {
	auto &plan = CreatePlan(*op.children[0]); // Generate child plan
	auto bf_creator = CreatePlanFromRelated(*op.related_create_bf);

	auto &bf_plan = op.filter_plan;
	size_t target_idx = std::numeric_limits<size_t>::max();
	for (size_t i = 0; i < bf_creator->filter_plans.size(); i++) {
		auto &filter_plan = bf_creator->filter_plans[i];
		if (Expression::ListEquals(filter_plan->apply, bf_plan->apply)) {
			target_idx = i;
			break; // Found the target, exit loop
		}
	}
	D_ASSERT(target_idx != std::numeric_limits<size_t>::max());

	auto &use_bf = Make<PhysicalUseBF>(plan.types, op.filter_plan, bf_creator->bf_to_create[target_idx], bf_creator,
	                                   op.estimated_cardinality);
	use_bf.children.emplace_back(plan);
	return use_bf;
}

} // namespace duckdb
