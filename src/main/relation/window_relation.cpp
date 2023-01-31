//
// Created by Tom Ebergen on 27/01/2023.
//

#include "duckdb/main/relation/window_relation.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

WindowRelation::WindowRelation(shared_ptr<Relation> rel, unique_ptr<ColumnRefExpression> child_, vector<ColumnRefExpression> partitions_)
    : Relation(rel->context, RelationType::PROJECTION_RELATION) {
//	vector<ColumnDefinition> dummy_columns;
	child = std::move(child_);
	type = RelationType::WINDOW_RELATION;
	for (auto p : partitions_) {
		partitions.emplace_back(p);
	}
	from_table = rel;
	context.GetContext()->TryBindRelation(*this, this->columns);
//	partitions = partitions_;
//	partitions = partitions;
//	bounds = bounds;
//	window->children??
	// window->partitions??
	// window->orders??
	// push back children
	// add partitions if necessary
	// add orders
}

unique_ptr<QueryNode> WindowRelation::GetQueryNode() {
//	select j, i, sum(i) over (partition by j) from a order by 1,2
// select j, i, sum(i) over (partition by j order by i) from a order by 1,2
	auto result = make_unique<SelectNode>();
	auto window_expr = make_unique<WindowExpression>(ExpressionType::WINDOW_AGGREGATE, "", schema_name, "sum");
	window_expr->children.push_back(make_unique<ColumnRefExpression>(child->GetColumnName(), child->GetTableName()));
	for (auto &partition : this->partitions) {
		auto yea = make_unique<ColumnRefExpression>(partition.GetColumnName(), partition.GetTableName());
		window_expr->partitions.push_back(std::move(yea));
	}

	// need to add support for more function names
	window_expr->function_name = "sum";

	// need to add window expression ranges
	window_expr->start = WindowBoundary::UNBOUNDED_PRECEDING;
	window_expr->end = WindowBoundary::CURRENT_ROW_RANGE;
	// window_expr->filter_expr ??
	// window_expr->start_expr
	// window_expr->end_expr
	// window_expr->offset_expr
	// window_expr->default_expr.
	result->select_list.push_back(make_unique<StarExpression>());
	result->select_list.push_back(std::move(window_expr));
	result->from_table = from_table->GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> WindowRelation::GetTableRef() {
	return nullptr;
}

const vector<ColumnDefinition> &WindowRelation::Columns() {
	return columns;
}

string WindowRelation::ToString(idx_t depth) {
	return "Window relation to string";
}

string WindowRelation::GetAlias() {
	return "window relation alias";
}

} // namespace duckdb
