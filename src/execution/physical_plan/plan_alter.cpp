#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_alter.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalAlter &op) {
	return Make<PhysicalAlter>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
