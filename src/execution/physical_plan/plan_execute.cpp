#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalExecute &op) {
	// TODO: Did I mess up the root here?
	if (op.prepared->rooty) {
		D_ASSERT(op.children.empty());
		return Make<PhysicalExecute>(*op.prepared->rooty);
	}

	D_ASSERT(op.children.size() == 1);
	auto &owned_plan = CreatePlan(*op.children[0]);
	auto &execute_ref = Make<PhysicalExecute>(owned_plan);
	auto &cast_execute_ref = execute_ref.Cast<PhysicalExecute>();
	cast_execute_ref.owned_plan = owned_plan;
	cast_execute_ref.prepared = op.prepared;
	return execute_ref;
}

// unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExecute &op) {
//	if (!op.prepared->plan) { // plan is nullptr, (ops are empty)
//		D_ASSERT(op.children.size() == 1);
//		auto owned_plan = CreatePlan(*op.children[0]);
//		auto execute = make_uniq<PhysicalExecute>(*owned_plan);
//		execute->owned_plan = std::move(owned_plan);
//		execute->prepared = std::move(op.prepared);
//		return std::move(execute);
//	} else {
//		D_ASSERT(op.children.size() == 0);
//		return make_uniq<PhysicalExecute>(*op.prepared->plan);
//	}
//}

} // namespace duckdb
