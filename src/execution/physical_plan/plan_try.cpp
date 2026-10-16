#include "duckdb/execution/operator/helper/physical_try.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_try.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalTry &op) {
	D_ASSERT(op.children.size() == 1);
	auto &plan = CreatePlan(*op.children[0]);
	auto &collector = Make<PhysicalTryCollector>(plan, op.estimated_cardinality);
	return Make<PhysicalTry>(collector, op.estimated_cardinality);
}

} // namespace duckdb
