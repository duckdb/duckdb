#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/schema/physical_create_index.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex &op) {

	D_ASSERT(op.children.empty());

	unique_ptr<TableFilterSet> table_filters;
	op.info->column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);

	auto &bind_data = (TableScanBindData &)*op.bind_data;
	bind_data.is_create_index = true;
	auto table_scan =
	    make_unique<PhysicalTableScan>(op.info->scan_types, op.function, move(op.bind_data), op.info->column_ids,
	                                   op.info->names, move(table_filters), op.estimated_cardinality);

	dependencies.insert(&op.table);
	op.info->column_ids.pop_back();

	auto physical_create_index =
	    make_unique<PhysicalCreateIndex>(op, op.table, op.info->column_ids, move(op.expressions), move(op.info),
	                                     move(op.unbound_expressions), op.estimated_cardinality);
	physical_create_index->children.push_back(move(table_scan));
	return move(physical_create_index);
}

} // namespace duckdb
