#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
PhysicalCreateBF *PhysicalPlanGenerator::CreatePlanFromRelated(LogicalCreateBF &op) {
	if (op.physical == nullptr) {
		unique_ptr<PhysicalOperator> plan = CreatePlan(*op.children[0]);
		PhysicalCreateBF *create_bf = new PhysicalCreateBF(plan->types, op.bf_to_create, op.estimated_cardinality);
		create_bf->children.emplace_back(std::move(plan));
		op.physical = create_bf;
		return create_bf;
	} else {
		return op.physical;
	}
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateBF &op) {
	unique_ptr<PhysicalCreateBF> create_bf;
	unique_ptr<PhysicalOperator> plan;
	if (op.physical == nullptr) {
		plan = CreatePlan(*op.children[0]);
		create_bf = make_uniq<PhysicalCreateBF>(plan->types, op.bf_to_create, op.estimated_cardinality);
		op.physical = create_bf.get();
		create_bf->children.emplace_back(std::move(plan));
	} else {
		create_bf = unique_ptr<PhysicalCreateBF>(op.physical);
	}
	plan = std::move(create_bf);
	return plan;
}
} // namespace duckdb
