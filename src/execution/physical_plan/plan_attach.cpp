#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_attach.hpp"
#include "duckdb/execution/operator/schema/physical_attach.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalAttach &op) {
	return Make<PhysicalAttach>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
