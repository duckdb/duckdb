#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_reset.hpp"
#include "duckdb/execution/operator/helper/physical_reset.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalReset &op) {
	return Make<PhysicalReset>(op.name, op.scope, op.estimated_cardinality);
}

} // namespace duckdb
