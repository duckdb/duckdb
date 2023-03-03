//
// Created by Tom Ebergen on 27/01/2023.
//

#include "duckdb/main/relation/window_relation.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

// static WindowBoundary StringToWindowBoundary(string &window_boundary) {
//	if (window_boundary == "unbounded_preceding") {
//		return WindowBoundary::UNBOUNDED_PRECEDING;
//	} else if (window_boundary == "unbounded_following") {
//		return WindowBoundary::UNBOUNDED_FOLLOWING;
//	} else if (window_boundary == "current_row_range") {
//		return WindowBoundary::CURRENT_ROW_RANGE;
//	} else if (window_boundary == "current_row_rows") {
//		return WindowBoundary::CURRENT_ROW_ROWS;
//	} else if (window_boundary == "expr_preceding_rows") {
//		return WindowBoundary::EXPR_PRECEDING_ROWS;
//	} else if (window_boundary == "expr_following_rows") {
//		return WindowBoundary::EXPR_FOLLOWING_ROWS;
//	} else if (window_boundary == "expr_preceding_range") {
//		return WindowBoundary::EXPR_PRECEDING_RANGE;
//	} else {
//		return WindowBoundary::EXPR_FOLLOWING_RANGE;
//	}
// }

WindowRelation::WindowRelation(shared_ptr<Relation> rel, std::string window_function, std::string window_alias,
                               vector<unique_ptr<ParsedExpression>> children_,
                               vector<unique_ptr<ParsedExpression>> partitions_, shared_ptr<OrderRelation> order_,
                               unique_ptr<ParsedExpression> filter_expr_, std::string window_boundary_start,
                               std::string window_boundary_end, unique_ptr<ParsedExpression> start_expr,
                               unique_ptr<ParsedExpression> end_expr, unique_ptr<ParsedExpression> offset_expr,
                               unique_ptr<ParsedExpression> default_expr)
    : Relation(rel->context, RelationType::WINDOW_RELATION), alias(window_alias), window_function(window_function),
      from_table(rel), start(StringToWindowBoundary(window_boundary_start)),
      end(StringToWindowBoundary(window_boundary_end)), start_expr(std::move(start_expr)),
      end_expr(std::move(end_expr)), offset_expr(std::move(offset_expr)), default_expr(std::move(default_expr)) {

	for (auto &child : children_) {
		children.push_back(std::move(child));
	}

	for (auto &partition : partitions_) {
		partitions.push_back(std::move(partition));
	}
	if (order_) {
		for (auto &actual_order : order_->orders) {
			orders.push_back(OrderByNode(actual_order.type, actual_order.null_order, actual_order.expression->Copy()));
		}
	}
	filter_expr = nullptr;
	if (filter_expr_) {
		filter_expr = filter_expr_ ? filter_expr_->Copy() : nullptr;
	}

	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> WindowRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	ExpressionType window_type = WindowToExpressionType(window_function);
	auto window_expr = make_unique<WindowExpression>(window_type, "", "", window_function);

	for (auto &child : children) {
		window_expr->children.push_back(child->Copy());
	}
	for (auto &partition : this->partitions) {
		window_expr->partitions.push_back(partition->Copy());
	}

	for (auto &order : this->orders) {
		window_expr->orders.push_back(OrderByNode(order.type, order.null_order, order.expression->Copy()));
	}

	// need to add support for more function names
	window_expr->function_name = window_function;

	// need to add window expression ranges
	window_expr->start = start;
	window_expr->end = end;

	if (filter_expr) {
		window_expr->filter_expr = filter_expr->Copy();
	} else {
		window_expr->filter_expr = nullptr;
	}
	window_expr->start_expr = start_expr ? start_expr->Copy() : nullptr;
	window_expr->end_expr = end_expr ? end_expr->Copy() : nullptr;
	window_expr->offset_expr = offset_expr ? offset_expr->Copy() : nullptr;
	window_expr->default_expr = default_expr ? default_expr->Copy() : nullptr;
	window_expr->alias = alias;

	result->select_list.push_back(make_unique<StarExpression>());
	result->select_list.push_back(std::move(window_expr));
	result->from_table = from_table->GetTableRef();

	for (auto &select : result->select_list) {
		table_ref_children.push_back(select->Copy());
	}

	return std::move(result);
}

unique_ptr<TableRef> WindowRelation::GetTableRef() {
	auto select_node = GetQueryNode();
	auto select_statement = make_unique<SelectStatement>();
	select_statement->node = std::move(select_node);
	auto ref = make_unique<SubqueryRef>(std::move(select_statement), GetAlias());
	return std::move(ref);
}

const vector<ColumnDefinition> &WindowRelation::Columns() {
	return columns;
}

string WindowRelation::ToString(idx_t depth) {
	return "Window this that cool";
}

string WindowRelation::GetAlias() {
	// set the alias, otherwise you end up with a really long alias name.
	return alias;
}

} // namespace duckdb
