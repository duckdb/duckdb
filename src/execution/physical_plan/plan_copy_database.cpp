#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_copy_database.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_database.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopyDatabase &op) {
	auto plan = CreatePlan(*op.children[0]);
	auto node = make_uniq<PhysicalCopyDatabase>(op.types, op.estimated_cardinality);
	node->children.push_back(std::move(plan));
	return std::move(node);
}

} // namespace duckdb
