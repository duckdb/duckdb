#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_drop.hpp"
#include "duckdb/execution/operator/schema/physical_drop.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalDrop &op) {
	return Make<PhysicalDrop>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
