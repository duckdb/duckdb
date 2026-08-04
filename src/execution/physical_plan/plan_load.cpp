#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_load.hpp"
#include "duckdb/execution/operator/helper/physical_load.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalLoad &op) {
	return Make<PhysicalLoad>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
