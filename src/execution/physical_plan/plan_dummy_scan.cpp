#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDummyScan &op) {
	D_ASSERT(op.children.size() == 0);
	return make_uniq<PhysicalDummyScan>(op.types, op.estimated_cardinality);
}

} // namespace duckdb
