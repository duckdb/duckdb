#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
PhysicalCreateBF *PhysicalPlanGenerator::CreatePlanFromRelated(LogicalCreateBF &op) {
	if (!op.physical) {
		auto plan = CreatePlan(*op.children[0]);
		auto create_bf = new PhysicalCreateBF(plan->types, op.bf_to_create_plans, op.estimated_cardinality);
		create_bf->children.emplace_back(std::move(plan));
		op.physical = create_bf; // Store the pointer safely
	}
	return op.physical;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateBF &op) {
	if (!op.physical) {
		auto plan = CreatePlan(*op.children[0]);
		auto create_bf = make_uniq<PhysicalCreateBF>(plan->types, op.bf_to_create_plans, op.estimated_cardinality);
		op.physical = create_bf.get(); // Ensure safe raw pointer storage
		create_bf->children.emplace_back(std::move(plan));
		return std::move(create_bf); // Transfer ownership safely
	}
	return unique_ptr<PhysicalOperator>(op.physical); // Ensure correct ownership
}

} // namespace duckdb
