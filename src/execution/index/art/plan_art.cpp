#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/schema/physical_create_art_index.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"

namespace duckdb {

PhysicalOperator &ART::CreatePlan(PlanIndexInput &input) {
	auto &op = input.op;
	auto &planner = input.planner;

	// PROJECTION on indexed columns.
	vector<LogicalType> new_column_types;
	vector<unique_ptr<Expression>> select_list;
	for (idx_t i = 0; i < op.expressions.size(); i++) {
		new_column_types.push_back(op.expressions[i]->return_type);
		select_list.push_back(std::move(op.expressions[i]));
	}
	new_column_types.emplace_back(LogicalType::ROW_TYPE);
	select_list.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, op.info->scan_types.size() - 1));

	auto &projection_ref =
	    planner.Make<PhysicalProjection>(new_column_types, std::move(select_list), op.estimated_cardinality);
	projection_ref.children.push_back(input.table_scan);

	// Optional NOT NULL filter.
	reference<PhysicalOperator> prev_operator_ref(projection_ref);
	auto is_alter = op.alter_table_info != nullptr;
	if (!is_alter) {
		vector<LogicalType> filter_types;
		vector<unique_ptr<Expression>> filter_select_list;
		auto not_null_type = ExpressionType::OPERATOR_IS_NOT_NULL;

		for (idx_t i = 0; i < new_column_types.size() - 1; i++) {
			filter_types.push_back(new_column_types[i]);
			auto is_not_null_expr = make_uniq<BoundOperatorExpression>(not_null_type, LogicalType::BOOLEAN);
			auto bound_ref = make_uniq<BoundReferenceExpression>(new_column_types[i], i);
			is_not_null_expr->children.push_back(std::move(bound_ref));
			filter_select_list.push_back(std::move(is_not_null_expr));
		}

		prev_operator_ref = planner.Make<PhysicalFilter>(std::move(filter_types), std::move(filter_select_list),
		                                                 op.estimated_cardinality);
		prev_operator_ref.get().types.emplace_back(LogicalType::ROW_TYPE);
		prev_operator_ref.get().children.push_back(projection_ref);
	}

	// Determine whether to push an ORDER BY operator.
	auto sort = true;
	if (op.unbound_expressions.size() > 1) {
		sort = false;
	} else if (op.unbound_expressions[0]->return_type.InternalType() == PhysicalType::VARCHAR) {
		sort = false;
	}

	// CREATE INDEX operator.
	auto &create_index_ref = planner.Make<PhysicalCreateARTIndex>(
	    op, op.table, op.info->column_ids, std::move(op.info), std::move(op.unbound_expressions),
	    op.estimated_cardinality, sort, std::move(op.alter_table_info));

	if (!sort) {
		create_index_ref.children.push_back(prev_operator_ref);
		return create_index_ref;
	}

	// ORDER BY operator.
	vector<BoundOrderByNode> orders;
	vector<idx_t> projections;
	for (idx_t i = 0; i < new_column_types.size() - 1; i++) {
		auto col_expr = make_uniq_base<Expression, BoundReferenceExpression>(new_column_types[i], i);
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, std::move(col_expr));
		projections.emplace_back(i);
	}
	projections.emplace_back(new_column_types.size() - 1);

	auto &order_ref = planner.Make<PhysicalOrder>(new_column_types, std::move(orders), std::move(projections),
	                                              op.estimated_cardinality);
	order_ref.children.push_back(prev_operator_ref);
	create_index_ref.children.push_back(order_ref);
	return create_index_ref;
}

} // namespace duckdb
