#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_copy_database.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_database.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopyDatabase &op) {
	auto node = make_uniq<PhysicalCopyDatabase>(op.types, op.estimated_cardinality, std::move(op.info));
	return std::move(node);
}

} // namespace duckdb
