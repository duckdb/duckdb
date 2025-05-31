#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_set.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/execution/operator/helper/physical_set_variable.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalSet &op) {
	// Set a config setting.
	if (op.children.empty()) {
		return Make<PhysicalSet>(op.name, op.value, op.scope, op.estimated_cardinality);
	}

	// Set a variable.
	auto &plan = CreatePlan(*op.children[0]);
	return Make<PhysicalSetVariable>(plan, std::move(op.name), op.estimated_cardinality);
}

} // namespace duckdb
