#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_delete.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> DuckCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                     unique_ptr<PhysicalOperator> plan) {
	// get the index of the row_id column
	auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();

	auto del = make_uniq<PhysicalDelete>(op.types, op.table, op.table.GetStorage(), std::move(op.bound_constraints),
	                                     bound_ref.index, op.estimated_cardinality, op.return_chunk);
	del->children.push_back(std::move(plan));
	return std::move(del);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelete &op) {
	D_ASSERT(op.children.size() == 1);
	D_ASSERT(op.expressions.size() == 1);
	D_ASSERT(op.expressions[0]->GetExpressionType() == ExpressionType::BOUND_REF);

	auto plan = CreatePlan(*op.children[0]);

	dependencies.AddDependency(op.table);
	return op.table.catalog.PlanDelete(context, op, std::move(plan));
}

} // namespace duckdb
