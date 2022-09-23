#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalInsert &op) {
	unique_ptr<PhysicalOperator> plan;
	if (!op.children.empty()) {
		D_ASSERT(op.children.size() == 1);
		plan = CreatePlan(*op.children[0]);
	}

	dependencies.insert(op.table);
	auto insert = make_unique<PhysicalInsert>(op.types, op.table, op.column_index_map, move(op.bound_defaults),
	                                          op.estimated_cardinality, op.return_chunk);
	if (plan) {
		insert->children.push_back(move(plan));
	}
	return move(insert);
}

} // namespace duckdb
