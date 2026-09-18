#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_disconnect.hpp"
#include "duckdb/execution/operator/helper/physical_disconnect.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalDisconnect &op) {
	return Make<PhysicalDisconnect>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
