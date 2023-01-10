#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_update.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalUpdate &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	dependencies.AddDependency(op.table);
	auto update = make_unique<PhysicalUpdate>(op.types, *op.table, *op.table->storage, op.columns, std::move(op.expressions),
	                                          std::move(op.bound_defaults), op.estimated_cardinality, op.return_chunk);

	update->update_is_del_and_insert = op.update_is_del_and_insert;
	update->children.push_back(std::move(plan));
	return std::move(update);
}

} // namespace duckdb
