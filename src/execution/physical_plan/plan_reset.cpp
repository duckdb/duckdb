#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_set.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalReset &op) {
	return make_unique<PhysicalReset>(op.name, op.scope, op.estimated_cardinality);
}

} // namespace duckdb
