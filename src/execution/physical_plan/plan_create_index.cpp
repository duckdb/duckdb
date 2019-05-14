#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "execution/operator/schema/physical_create_index.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_create_index.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex &op) {
	assert(op.children.size() == 0);
	dependencies.insert(&op.table);
	return make_unique<PhysicalCreateIndex>(op, op.table, op.column_ids, move(op.expressions), move(op.info),
	                                        move(op.unbound_expressions));
}
