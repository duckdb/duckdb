#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_delete.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelete &op) {
	D_ASSERT(op.children.size() == 1);
	D_ASSERT(op.expressions.size() == 1);
	D_ASSERT(op.expressions[0]->type == ExpressionType::BOUND_REF);

	auto plan = CreatePlan(*op.children[0]);

	// get the index of the row_id column
	auto &bound_ref = (BoundReferenceExpression &)*op.expressions[0];

	dependencies.AddDependency(op.table);
	auto del = make_unique<PhysicalDelete>(op.types, *op.table, *op.table->storage, bound_ref.index,
	                                       op.estimated_cardinality, op.return_chunk);
	del->children.push_back(move(plan));
	return move(del);
}

} // namespace duckdb
