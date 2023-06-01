#include "duckdb/execution/operator/projection/physical_unnest.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalUnnest &op) {
	D_ASSERT(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);
	auto unnest = make_uniq<PhysicalUnnest>(op.types, std::move(op.expressions), op.estimated_cardinality);
	unnest->children.push_back(std::move(plan));
	return std::move(unnest);
}

} // namespace duckdb
