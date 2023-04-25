#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_update.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> DuckCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                     unique_ptr<PhysicalOperator> plan) {
	auto update =
	    make_uniq<PhysicalUpdate>(op.types, op.table, op.table.GetStorage(), op.columns, std::move(op.expressions),
	                              std::move(op.bound_defaults), op.estimated_cardinality, op.return_chunk);

	update->update_is_del_and_insert = op.update_is_del_and_insert;
	update->children.push_back(std::move(plan));
	return std::move(update);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalUpdate &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	dependencies.AddDependency(op.table);
	return op.table.catalog.PlanUpdate(context, op, std::move(plan));
}

} // namespace duckdb
