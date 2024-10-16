#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_set.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/execution/operator/helper/physical_set_variable.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSet &op) {
	if (!op.children.empty()) {
		// set variable
		auto child = CreatePlan(*op.children[0]);
		auto set_variable = make_uniq<PhysicalSetVariable>(std::move(op.name), op.estimated_cardinality);
		set_variable->children.push_back(std::move(child));
		return std::move(set_variable);
	}
	// set config setting
	return make_uniq<PhysicalSet>(op.name, op.value, op.scope, op.estimated_cardinality);
}

} // namespace duckdb
