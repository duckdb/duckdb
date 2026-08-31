#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_transaction.hpp"
#include "duckdb/execution/operator/helper/physical_transaction.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalTransaction &op) {
	return Make<PhysicalTransaction>(std::move(op.info), op.estimated_cardinality);
}

} // namespace duckdb
