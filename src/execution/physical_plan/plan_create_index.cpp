#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/schema/physical_create_art_index.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex &op) {
	// generate a physical plan for the parallel index creation which consists of the following operators
	// table scan - projection (for expression execution) - filter (NOT NULL) - order (if applicable) - create index

	D_ASSERT(op.children.size() == 1);
	auto table_scan = CreatePlan(*op.children[0]);

	// validate that all expressions contain valid scalar functions
	// e.g. get_current_timestamp(), random(), and sequence values are not allowed as index keys
	// because they make deletions and lookups unfeasible
	for (idx_t i = 0; i < op.unbound_expressions.size(); i++) {
		auto &expr = op.unbound_expressions[i];
		if (!expr->IsConsistent()) {
			throw BinderException("Index keys cannot contain expressions with side effects.");
		}
	}

	// if we get here and the index type is not ART, we throw an exception
	// because we don't support any other index type yet. However, an operator extension could have
	// replaced this part of the plan with a different index creation operator.
	if (op.info->index_type != ART::TYPE_NAME) {
		throw BinderException("Unknown index type: " + op.info->index_type);
	}

	// table scan operator for index key columns and row IDs
	dependencies.AddDependency(op.table);

	D_ASSERT(op.info->scan_types.size() - 1 <= op.info->names.size());
	D_ASSERT(op.info->scan_types.size() - 1 <= op.info->column_ids.size());

	// projection to execute expressions on the key columns

	vector<LogicalType> new_column_types;
	vector<unique_ptr<Expression>> select_list;
	for (idx_t i = 0; i < op.expressions.size(); i++) {
		new_column_types.push_back(op.expressions[i]->return_type);
		select_list.push_back(std::move(op.expressions[i]));
	}
	new_column_types.emplace_back(LogicalType::ROW_TYPE);
	select_list.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, op.info->scan_types.size() - 1));

	auto projection = make_uniq<PhysicalProjection>(new_column_types, std::move(select_list), op.estimated_cardinality);
	projection->children.push_back(std::move(table_scan));

	// filter operator for IS_NOT_NULL on each key column

	vector<LogicalType> filter_types;
	vector<unique_ptr<Expression>> filter_select_list;

	for (idx_t i = 0; i < new_column_types.size() - 1; i++) {
		filter_types.push_back(new_column_types[i]);
		auto is_not_null_expr =
		    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		auto bound_ref = make_uniq<BoundReferenceExpression>(new_column_types[i], i);
		is_not_null_expr->children.push_back(std::move(bound_ref));
		filter_select_list.push_back(std::move(is_not_null_expr));
	}

	auto null_filter =
	    make_uniq<PhysicalFilter>(std::move(filter_types), std::move(filter_select_list), op.estimated_cardinality);
	null_filter->types.emplace_back(LogicalType::ROW_TYPE);
	null_filter->children.push_back(std::move(projection));

	// determine if we sort the data prior to index creation
	// we don't sort, if either VARCHAR or compound key
	auto perform_sorting = true;
	if (op.unbound_expressions.size() > 1) {
		perform_sorting = false;
	} else if (op.unbound_expressions[0]->return_type.InternalType() == PhysicalType::VARCHAR) {
		perform_sorting = false;
	}

	// actual physical create index operator

	auto physical_create_index =
	    make_uniq<PhysicalCreateARTIndex>(op, op.table, op.info->column_ids, std::move(op.info),
	                                      std::move(op.unbound_expressions), op.estimated_cardinality, perform_sorting);

	if (perform_sorting) {

		// optional order operator
		vector<BoundOrderByNode> orders;
		vector<idx_t> projections;
		for (idx_t i = 0; i < new_column_types.size() - 1; i++) {
			auto col_expr = make_uniq_base<Expression, BoundReferenceExpression>(new_column_types[i], i);
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, std::move(col_expr));
			projections.emplace_back(i);
		}
		projections.emplace_back(new_column_types.size() - 1);

		auto physical_order = make_uniq<PhysicalOrder>(new_column_types, std::move(orders), std::move(projections),
		                                               op.estimated_cardinality);
		physical_order->children.push_back(std::move(null_filter));

		physical_create_index->children.push_back(std::move(physical_order));
	} else {

		// no ordering
		physical_create_index->children.push_back(std::move(null_filter));
	}

	return std::move(physical_create_index);
}

} // namespace duckdb
