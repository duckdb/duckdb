#include "duckdb/execution/operator/helper/physical_ussr_insertion.h"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_ussr_insertion.h"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalUSSRInsertion &op) {
	D_ASSERT(op.children.size() == 1);
	auto &plan = CreatePlan(*op.children[0]);

	auto &ussr_insertion =
	    Make<PhysicalUnifiedString>(op.types, std::move(op.insert_to_ussr), op.estimated_cardinality);
	ussr_insertion.children.push_back(plan);

	return ussr_insertion;
}

} // namespace duckdb
