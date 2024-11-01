#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/schema/physical_create_art_index.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/execution/index/index_type.hpp"
#include "duckdb/execution/index/bound_index.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex &op) {

	// validate that all expressions contain valid scalar functions
	// e.g. get_current_timestamp(), random(), and sequence values are not allowed as index keys
	// because they make deletions and lookups unfeasible
	for (idx_t i = 0; i < op.unbound_expressions.size(); i++) {
		auto &expr = op.unbound_expressions[i];
		if (!expr->IsConsistent()) {
			throw BinderException("Index keys cannot contain expressions with side effects.");
		}
	}

	// Do we have a valid index type?
	const auto index_type = context.db->config.GetIndexTypes().FindByName(op.info->index_type);
	if (!index_type) {
		throw BinderException("Unknown index type: " + op.info->index_type);
	}
	if (!index_type->create_plan) {
		throw InternalException("Index type '%s' is missing a create_plan function", op.info->index_type);
	}

	// table scan operator for index key columns and row IDs
	dependencies.AddDependency(op.table);

	D_ASSERT(op.info->scan_types.size() - 1 <= op.info->names.size());
	D_ASSERT(op.info->scan_types.size() - 1 <= op.info->column_ids.size());

	D_ASSERT(op.children.size() == 1);
	auto table_scan = CreatePlan(*op.children[0]);

	PlanIndexInput input(context, op, table_scan);
	return index_type->create_plan(input);
}

} // namespace duckdb
