#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb
{
unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCrossProduct &op)
{
	D_ASSERT(op.children.size() == 2);
	LogicalOperator* left_pop = (LogicalOperator*)op.children[0].get();
	auto left = CreatePlan(*left_pop);
	LogicalOperator* right_pop = (LogicalOperator*)op.children[0].get();
	auto right = CreatePlan(*right_pop);
	return make_uniq<PhysicalCrossProduct>(op.types, std::move(left), std::move(right), op.estimated_cardinality);
}
} // namespace duckdb