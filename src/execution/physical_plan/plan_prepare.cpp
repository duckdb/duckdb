#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_prepare.hpp"
#include "duckdb/execution/operator/helper/physical_prepare.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalPrepare &op) {
	// Generate the physical plan only if all parameters are bound.
	// Otherwise, the physical plan is never used.
	D_ASSERT(op.children.size() <= 1);
	if (op.prepared->properties.bound_all_parameters && !op.children.empty()) {
		auto &plan = CreatePlan(*op.children[0]);
		op.prepared->types = plan.types;
		op.prepared->root = plan;
	}
	return Make<PhysicalPrepare>(op.name, std::move(op.prepared), op.estimated_cardinality);
}

} // namespace duckdb
