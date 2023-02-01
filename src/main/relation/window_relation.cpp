//
// Created by Tom Ebergen on 27/01/2023.
//

#include "duckdb/main/relation/window_relation.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

WindowRelation::WindowRelation(shared_ptr<Relation> rel,
                               vector<unique_ptr<ParsedExpression>> children_,
                               vector<unique_ptr<ParsedExpression>> partitions_,
                               vector<unique_ptr<OrderByNode>> orders_, unique_ptr<ParsedExpression> filter_expr_,
                               WindowBoundary start_,
                               WindowBoundary end_,
                               vector<unique_ptr<ParsedExpression>> start_end_offset_default)
    : Relation(rel->context, RelationType::PROJECTION_RELATION) {

	for (auto &child: children_) {
		children.push_back(std::move(child));
	}
	type = RelationType::WINDOW_RELATION;
	for (auto &partition: partitions_) {
		partitions.push_back(std::move(partition));
	}
	for (auto &o : orders_) {
		orders.emplace_back(make_unique<OrderByNode>(o->type, o->null_order, o->expression->Copy()));
	}
	function_name = "sum";
	from_table = rel;

	filter_expr = filter_expr_ ? filter_expr_->Copy() : nullptr;

	start = start_;
	end = end_;

	start_expr = nullptr;
	end_expr = nullptr;
	offset_expr = nullptr;
	default_expr = nullptr;

	idx_t i = 0;
	while (i < 4) {
		if (i == 0 && start_end_offset_default.size() >= 1 && start_end_offset_default.at(i) != nullptr) {
			start_expr = std::move(start_end_offset_default.at(i));
		}
		if (i == 1 && start_end_offset_default.size() >= 2 && start_end_offset_default.at(i) != nullptr) {
			end_expr = std::move(start_end_offset_default.at(i));
		}
		if (i == 2 && start_end_offset_default.size() >= 3 && start_end_offset_default.at(i) != nullptr) {
			offset_expr = std::move(start_end_offset_default.at(i));
		}
		if (i == 3 && start_end_offset_default.size() >= 4 && start_end_offset_default.at(i) != nullptr) {
			default_expr = std::move(start_end_offset_default.at(i));
		}
		i += 1;
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> WindowRelation::GetQueryNode() {
//	select j, i, sum(i) over (partition by j) from a order by 1,2
// select j, i, sum(i) over (partition by j order by i) from a order by 1,2
	auto result = make_unique<SelectNode>();
	auto window_expr = make_unique<WindowExpression>(ExpressionType::WINDOW_AGGREGATE, "", schema_name, "sum");
	for (auto &child : children) {
		window_expr->children.push_back(child->Copy());
	}
	for (auto &partition : this->partitions) {
		window_expr->partitions.push_back(partition->Copy());
	}

	// need to add support for more function names
	window_expr->function_name = "sum";

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
	window_expr->alias = "ALIAS";

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
	return "WINDOW_ALIAS";
}

} // namespace duckdb
