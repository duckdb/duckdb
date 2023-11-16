#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_secret.hpp"
#include "duckdb/execution/operator/helper/physical_create_secret.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateSecret &op) {
	return make_uniq<PhysicalCreateSecret>(op.info, op.estimated_cardinality);
}

} // namespace duckdb
