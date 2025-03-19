#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_delete.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

namespace duckdb {

PhysicalOperator &DuckCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                          PhysicalOperator &plan) {
	// Get the row_id column index.
	auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
	auto &del = planner.Make<PhysicalDelete>(op.types, op.table, op.table.GetStorage(), std::move(op.bound_constraints),
	                                         bound_ref.index, op.estimated_cardinality, op.return_chunk);
	del.children.push_back(plan);
	return del;
}

PhysicalOperator &Catalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) {
	auto &plan = planner.CreatePlan(*op.children[0]);
	return PlanDelete(context, planner, op, plan);
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalDelete &op) {
	D_ASSERT(op.children.size() == 1);
	D_ASSERT(op.expressions.size() == 1);
	D_ASSERT(op.expressions[0]->GetExpressionType() == ExpressionType::BOUND_REF);

	dependencies.AddDependency(op.table);
	return op.table.catalog.PlanDelete(context, *this, op);
}

} // namespace duckdb
