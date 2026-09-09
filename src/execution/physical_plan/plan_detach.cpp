#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_detach.hpp"
#include "duckdb/execution/operator/schema/physical_detach.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalDetach &op) {
	return Make<PhysicalDetach>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
