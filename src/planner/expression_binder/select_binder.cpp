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

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                           const case_insensitive_map_t<idx_t> &alias_map, BoundGroupInformation &info)
    : ExpressionBinder(binder, context), column_alias_lookup(alias_map), column_alias_binder(node),
      inside_window(false), node(node), info(info) {
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
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef((ColumnRefExpression &)expr, depth, root_expression);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

BindResult SelectBinder::BindColumnRef(ColumnRefExpression &expr, idx_t depth, bool root_expression) {
	auto result = ExpressionBinder::BindExpression(expr, depth);
	if (!result.HasError()) {
		return result;
	}

	auto alias_index = column_alias_lookup.TryBindAlias(expr);
	if (alias_index != DConstants::INVALID_INDEX) {
		return column_alias_binder.BindAliasByDuplicatingParsedTarget(this, (ParsedExpression &)expr, alias_index,
		                                                              depth, root_expression);
	}
	return BindResult(StringUtil::Format("%s, and no existing column for \"%s\" found", result.error, expr.ToString()));
}

//! Attempt to bind to a grouping variable.
//! Aliases must be resolved with QualifyColumnNames before calling this function.
idx_t SelectBinder::TryBindGroup(ParsedExpression &expr, idx_t depth) {
	// check aliases for a match
	ParsedExpression *resolved_expr = &expr;

	unique_ptr<ParsedExpression> _resolved_expr;
	auto alias_index = column_alias_lookup.TryBindAlias(expr);
	if (alias_index != DConstants::INVALID_INDEX) {
		_resolved_expr = column_alias_binder.ResolveAliasByDuplicatingParsedTarget(alias_index);
		resolved_expr = &*_resolved_expr;
	}

	// check the list of group columns for a match
	auto entry = info.map.find(resolved_expr);
	if (entry != info.map.end()) {
		return entry->second;
	}
#ifdef DEBUG
	for (auto entry : info.map) {
		D_ASSERT(!entry.first->Equals(resolved_expr));
		D_ASSERT(!resolved_expr->Equals(entry.first));
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
