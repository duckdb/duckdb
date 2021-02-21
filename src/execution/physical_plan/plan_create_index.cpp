#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/schema/physical_create_index.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex &op) {
	D_ASSERT(op.children.empty());
	dependencies.insert(&op.table);
	return make_unique<PhysicalCreateIndex>(op, op.table, op.column_ids, move(op.expressions), move(op.info),
	                                        move(op.unbound_expressions), op.estimated_cardinality);
}

} // namespace duckdb
