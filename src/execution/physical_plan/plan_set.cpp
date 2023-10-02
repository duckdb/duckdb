#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_set.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSet &op) {
	return make_uniq<PhysicalSet>(op.name, op.value, op.scope, op.estimated_cardinality);
}

} // namespace duckdb
