#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"
#include "duckdb/execution/operator/filter/physical_use_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalUseBF &op) {
	unique_ptr<PhysicalOperator> plan = CreatePlan(*op.children[0]);
	auto use_bf = make_uniq<PhysicalUseBF>(plan->types, op.bf_to_use, op.estimated_cardinality);
	use_bf->children.emplace_back(std::move(plan));
	for (auto cell : op.related_create_bf) {
		use_bf->related_create_bf.emplace_back(CreatePlanFromRelated(*cell));
	}
	plan = std::move(use_bf);
	return plan;
}
} // namespace duckdb