#include "duckdb/planner/expression_binder/select_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info)
    : ExpressionBinder(binder, context), inside_window(false), node(node), info(info) {
}

BindResult SelectBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr, depth);
	if (group_index != DConstants::INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::DEFAULT:
		return BindResult("SELECT clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindWindow((WindowExpression &)expr, depth);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

idx_t SelectBinder::TryBindGroup(ParsedExpression &expr, idx_t depth) {
	// first check the group alias map, if expr is a ColumnRefExpression
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)expr;
		if (!colref.IsQualified()) {
			auto alias_entry = info.alias_map.find(colref.column_names[0]);
			if (alias_entry != info.alias_map.end()) {
				// found entry!
				return alias_entry->second;
			}
		}
	}
	// no alias reference found
	// check the list of group columns for a match
	auto entry = info.map.find(&expr);
	if (entry != info.map.end()) {
		return entry->second;
	}
#ifdef DEBUG
	for (auto entry : info.map) {
		D_ASSERT(!entry.first->Equals(&expr));
		D_ASSERT(!expr.Equals(entry.first));
	}
#endif
	return DConstants::INVALID_INDEX;
}

BindResult SelectBinder::BindGroupingFunction(OperatorExpression &op, idx_t depth) {
	if (op.children.empty()) {
		throw InternalException("GROUPING requires at least one child");
	}
	if (node.groups.group_expressions.empty()) {
		return BindResult(binder.FormatError(op, "GROUPING statement cannot be used without groups"));
	}
	if (op.children.size() >= 64) {
		return BindResult(binder.FormatError(op, "GROUPING statement cannot have more than 64 groups"));
	}
	vector<idx_t> group_indexes;
	group_indexes.reserve(op.children.size());
	for (auto &child : op.children) {
		ExpressionBinder::QualifyColumnNames(binder, child);
		auto idx = TryBindGroup(*child, depth);
		if (idx == DConstants::INVALID_INDEX) {
			return BindResult(binder.FormatError(
			    op, StringUtil::Format("GROUPING child \"%s\" must be a grouping column", child->GetName())));
		}
		group_indexes.push_back(idx);
	}
	auto col_idx = node.grouping_functions.size();
	node.grouping_functions.push_back(move(group_indexes));
	return BindResult(make_unique<BoundColumnRefExpression>(op.GetName(), LogicalType::BIGINT,
	                                                        ColumnBinding(node.groupings_index, col_idx), depth));
}

BindResult SelectBinder::BindGroup(ParsedExpression &expr, idx_t depth, idx_t group_index) {
	auto &group = node.groups.group_expressions[group_index];
	return BindResult(make_unique<BoundColumnRefExpression>(expr.GetName(), group->return_type,
	                                                        ColumnBinding(node.group_index, group_index), depth));
}

} // namespace duckdb
