#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_update.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

namespace duckdb {

PhysicalOperator &DuckCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                          PhysicalOperator &plan) {
	auto &update_ref = planner.Make<PhysicalUpdate>(
	    op.types, op.table, op.table.GetStorage(), op.columns, std::move(op.expressions), std::move(op.bound_defaults),
	    std::move(op.bound_constraints), op.estimated_cardinality, op.return_chunk);
	auto &cast_update_ref = update_ref.Cast<PhysicalUpdate>();
	cast_update_ref.update_is_del_and_insert = op.update_is_del_and_insert;
	cast_update_ref.children.push_back(plan);
	return update_ref;
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalUpdate &op) {
	D_ASSERT(op.children.size() == 1);
	auto &plan_ref = CreatePlan(*op.children[0]);
	dependencies.AddDependency(op.table);
	return op.table.catalog.PlanUpdate(context, *this, op, plan_ref);
}

} // namespace duckdb
