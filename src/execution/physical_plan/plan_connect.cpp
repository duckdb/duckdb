#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_connect.hpp"
#include "duckdb/execution/operator/helper/physical_connect.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalConnect &op) {
	return Make<PhysicalConnect>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
