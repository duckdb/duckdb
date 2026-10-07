#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/constants.hpp"
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
	// Convert storage-column-to-expression mappings to storage-column-to-chunk mappings.
	auto return_columns = std::move(op.return_columns);
	for (auto &return_column_idx : return_columns) {
		if (return_column_idx == DConstants::INVALID_INDEX) {
			continue;
		}
		const auto expression_idx = return_column_idx;
		D_ASSERT(expression_idx < op.expressions.size());
		return_column_idx = op.expressions[expression_idx]->Cast<BoundReferenceExpression>().Index();
	}
	auto &del = planner.Make<PhysicalDelete>(op.types, op.table.Cast<DuckTableEntry>(), op.table.GetStorage(),
	                                         std::move(op.bound_constraints), bound_ref.Index(),
	                                         op.estimated_cardinality, op.return_chunk, std::move(return_columns));
	del.children.push_back(plan);
	return del;
}

PhysicalOperator &Catalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) {
	auto &plan = planner.CreatePlan(*op.children[0]);
	return PlanDelete(context, planner, op, plan);
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalDelete &op) {
	D_ASSERT(op.children.size() == 1);

	dependencies.AddDependency(op.table);
	return op.table.catalog.PlanDelete(context, *this, op);
}

} // namespace duckdb
