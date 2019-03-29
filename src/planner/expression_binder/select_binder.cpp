#include "planner/expression_binder/select_binder.hpp"

#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/window_expression.hpp"
#include "parser/parsed_expression_iterator.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression/bound_window_expression.hpp"
#include "planner/expression_binder/aggregate_binder.hpp"
#include "planner/query_node/bound_select_node.hpp"

using namespace duckdb;
using namespace std;

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                           expression_map_t<uint32_t> &group_map, unordered_map<string, uint32_t> &group_alias_map)
    : ExpressionBinder(binder, context), inside_window(false), node(node), group_map(group_map),
      group_alias_map(group_alias_map) {
}

BindResult SelectBinder::BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression) {
	// check if the expression binds to one of the groups
	auto group_binding = TryBindGroup(expr, depth);
	if (group_binding) {
		return BindResult(move(group_binding));
	}
	switch (expr.expression_class) {
	case ExpressionClass::DEFAULT:
		return BindResult("SELECT clause cannot contain DEFAULT clause");
	case ExpressionClass::AGGREGATE:
		return BindAggregate((AggregateExpression &)expr, depth);
	case ExpressionClass::WINDOW:
		return BindWindow((WindowExpression &)expr, depth);
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

unique_ptr<Expression> SelectBinder::TryBindGroup(ParsedExpression &expr, uint32_t depth) {
	uint32_t group_entry = (uint32_t)-1;
	bool found_group = false;
	// first check the group alias map, if expr is a ColumnRefExpression
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)expr;
		if (colref.table_name.empty()) {
			auto alias_entry = group_alias_map.find(colref.column_name);
			if (alias_entry != group_alias_map.end()) {
				// found entry!
				group_entry = alias_entry->second;
				found_group = true;
			}
		}
	}
	// now check the list of group columns for a match
	if (!found_group) {
		auto entry = group_map.find(&expr);
		if (entry != group_map.end()) {
			group_entry = entry->second;
			found_group = true;
		}
	}
	if (!found_group) {
		return nullptr;
	}
	auto &group = node.groups[group_entry];
	return make_unique<BoundColumnRefExpression>(expr.GetName(), group->return_type,
	                                             ColumnBinding(node.group_index, group_entry), group->sql_type, depth);
}
