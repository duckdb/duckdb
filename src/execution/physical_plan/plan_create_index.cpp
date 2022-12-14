#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/schema/physical_create_index.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex &op) {

	D_ASSERT(op.children.empty());

	// table scan operator for index key columns and row IDs
	unique_ptr<TableFilterSet> table_filters;
	op.info->column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);

	auto &bind_data = (TableScanBindData &)*op.bind_data;
	bind_data.is_create_index = true;

	auto table_scan =
	    make_unique<PhysicalTableScan>(op.info->scan_types, op.function, move(op.bind_data), op.info->column_ids,
	                                   op.info->names, move(table_filters), op.estimated_cardinality);

	dependencies.AddDependency(&op.table);
	op.info->column_ids.pop_back();

	D_ASSERT(op.info->scan_types.size() - 1 <= op.info->names.size());
	D_ASSERT(op.info->scan_types.size() - 1 <= op.info->column_ids.size());

	// filter operator for IS_NOT_NULL on each key column
	vector<LogicalType> filter_types;
	vector<unique_ptr<Expression>> filter_select_list;

	for (idx_t i = 0; i < op.info->scan_types.size() - 1; i++) {
		filter_types.push_back(op.info->scan_types[i]);
		auto is_not_null_expr =
		    make_unique<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		auto bound_ref =
		    make_unique<BoundReferenceExpression>(op.info->names[op.info->column_ids[i]], op.info->scan_types[i], i);
		is_not_null_expr->children.push_back(move(bound_ref));
		filter_select_list.push_back(move(is_not_null_expr));
	}

	auto null_filter = make_unique<PhysicalFilter>(move(filter_types), move(filter_select_list), STANDARD_VECTOR_SIZE);
	null_filter->types.emplace_back(LogicalType::ROW_TYPE);
	null_filter->children.push_back(move(table_scan));

	// actual physical create index operator
	auto physical_create_index =
	    make_unique<PhysicalCreateIndex>(op, op.table, op.info->column_ids, move(op.expressions), move(op.info),
	                                     move(op.unbound_expressions), op.estimated_cardinality);
	physical_create_index->children.push_back(move(null_filter));
	return move(physical_create_index);
}

} // namespace duckdb
