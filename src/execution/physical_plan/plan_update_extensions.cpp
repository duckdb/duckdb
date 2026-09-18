#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_update_extensions.hpp"
#include "duckdb/execution/operator/helper/physical_update_extensions.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalUpdateExtensions &op) {
	return Make<PhysicalUpdateExtensions>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
