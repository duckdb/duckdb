#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_copy_database.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_database.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalCopyDatabase &op) {
	return Make<PhysicalCopyDatabase>(op.types, op.estimated_cardinality, std::move(op.info));
}

} // namespace duckdb
