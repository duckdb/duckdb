//
// Created by Tom Ebergen on 27/01/2023.
//

#include "duckdb/main/relation/window_relation.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

WindowRelation::WindowRelation(shared_ptr<AggregateRelation> aggr)
    : Relation(aggr->context, RelationType::PROJECTION_RELATION) {
	vector<ColumnDefinition> dummy_columns;
	context.GetContext()->TryBindRelation(*this, dummy_columns);
	type = RelationType::WINDOW_RELATION;
//	window->children??
	// window->partitions??
	// window->orders??
	// push back children
	// add partitions if necessary
	// add orders
	// Look at WindowExpression::Copy();
}

unique_ptr<QueryNode> WindowRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	auto window_expr = make_unique<WindowExpression>(ExpressionType::WINDOW_AGGREGATE, "", schema_name, "sum");
	for (auto &child : this->children) {
		window_expr->children.push_back(child->Copy());
	}
	// window_expr->children
	// window_expr->partitions ??
	// window_expr->orders ??
	// window_expr->filter_expr ??
	// window_expr->start_expr
	// window_expr->end_expr
	// window_expr->offset_expr
	// window_expr->default_expr.
	result->select_list.push_back(std::move(window_expr));
	return std::move(result);
}

unique_ptr<TableRef> WindowRelation::GetTableRef() {
	return nullptr;
}

const vector<ColumnDefinition> &WindowRelation::Columns() {
	return columns;
}

string WindowRelation::ToString(idx_t depth) {
	return "";
}

string WindowRelation::GetAlias() {
	return "";
}

} // namespace duckdb
