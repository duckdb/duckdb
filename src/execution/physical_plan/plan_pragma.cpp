#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_pragma.hpp"

#include "duckdb/execution/operator/helper/physical_pragma.hpp"
namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalPragma &op) {
	return Make<PhysicalPragma>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
