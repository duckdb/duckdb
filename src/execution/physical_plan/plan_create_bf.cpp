#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
PhysicalCreateBF *PhysicalPlanGenerator::CreatePlanFromRelated(LogicalCreateBF &op) {
	if (!op.physical) {
		auto &plan = CreatePlan(*op.children[0]);
		auto &create_bf = Make<PhysicalCreateBF>(plan.types, op.filter_plans, op.min_max_to_create,
		                                         op.min_max_applied_cols, op.estimated_cardinality);
		create_bf.children.emplace_back(plan);
		op.physical = static_cast<PhysicalCreateBF *>(&create_bf); // Store the pointer safely
	}
	return op.physical;
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalCreateBF &op) {
	if (!op.physical) {
		auto &plan = CreatePlan(*op.children[0]);
		auto &create_bf = Make<PhysicalCreateBF>(plan.types, op.filter_plans, op.min_max_to_create,
		                                         op.min_max_applied_cols, op.estimated_cardinality);
		op.physical = static_cast<PhysicalCreateBF *>(&create_bf); // Ensure safe raw pointer storage
		create_bf.children.emplace_back(plan);
		return create_bf; // Transfer ownership safely
	}
	return *op.physical; // Ensure correct ownership
}

} // namespace duckdb
