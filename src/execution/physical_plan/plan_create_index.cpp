#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex &op) {
	// Early-out, if the index already exists.
	auto &schema = op.table.schema;
	auto entry = schema.GetEntry(schema.GetCatalogTransaction(context), CatalogType::INDEX_ENTRY, op.info->index_name);
	if (entry) {
		if (op.info->on_conflict != OnCreateConflict::IGNORE_ON_CONFLICT) {
			throw CatalogException("Index with name \"%s\" already exists!", op.info->index_name);
		}
		return Make<PhysicalDummyScan>(op.types, op.estimated_cardinality);
	}

	// Ensure that all expressions contain valid scalar functions.
	// E.g., get_current_timestamp(), random(), and sequence values cannot be index keys.
	for (idx_t i = 0; i < op.unbound_expressions.size(); i++) {
		auto &expr = op.unbound_expressions[i];
		if (!expr->IsConsistent()) {
			throw BinderException("Index keys cannot contain expressions with side effects.");
		}
	}

	// If we get here and the index type is not valid index type, we throw an exception.
	const auto index_type = context.db->config.GetIndexTypes().FindByName(op.info->index_type);
	if (!index_type) {
		throw BinderException("Unknown index type: " + op.info->index_type);
	}
	if (!index_type->create_plan) {
		throw InternalException("Index type '%s' is missing a create_plan function", op.info->index_type);
	}

	// Add a dependency for the entire table on which we create the index.
	dependencies.AddDependency(op.table);
	D_ASSERT(op.info->scan_types.size() - 1 <= op.info->names.size());
	D_ASSERT(op.info->scan_types.size() - 1 <= op.info->column_ids.size());

	// Generate a physical plan for the parallel index creation.
	// TABLE SCAN - PROJECTION - (optional) NOT NULL FILTER - (optional) ORDER BY - CREATE INDEX
	D_ASSERT(op.children.size() == 1);
	auto &plan = CreatePlan(*op.children[0]);

	PlanIndexInput input(context, op, *this, plan);
	return index_type->create_plan(input);
}

} // namespace duckdb
