#include "duckdb/planner/expression_binder/base_select_binder.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

BaseSelectBinder::BaseSelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                                   BoundGroupInformation &info, case_insensitive_map_t<idx_t> alias_map)
    : ExpressionBinder(binder, context), inside_window(false), node(node), info(info), alias_map(std::move(alias_map)) {
}

BaseSelectBinder::BaseSelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                                   BoundGroupInformation &info)
    : BaseSelectBinder(binder, context, node, info, case_insensitive_map_t<idx_t>()) {
}

BindResult BaseSelectBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr);
	if (group_index != DConstants::INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth);
	case ExpressionClass::DEFAULT:
		return BindResult("SELECT clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindWindow(expr.Cast<WindowExpression>(), depth);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth, root_expression);
	}
}

idx_t BaseSelectBinder::TryBindGroup(ParsedExpression &expr) {
	// first check the group alias map, if expr is a ColumnRefExpression
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
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
	auto entry = info.map.find(expr);
	if (entry != info.map.end()) {
		return entry->second;
	}
#ifdef DEBUG
	for (auto map_entry : info.map) {
		D_ASSERT(!map_entry.first.get().Equals(expr));
		D_ASSERT(!expr.Equals(map_entry.first.get()));
	}
#endif
	return DConstants::INVALID_INDEX;
}

BindResult BaseSelectBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth) {
	// first try to bind the column reference regularly
	auto result = ExpressionBinder::BindExpression(expr_ptr, depth);
	if (!result.HasError()) {
		return result;
	}
	// binding failed
	// check in the alias map
	auto &colref = (expr_ptr.get())->Cast<ColumnRefExpression>();
	if (!colref.IsQualified()) {
		auto alias_entry = alias_map.find(colref.column_names[0]);
		if (alias_entry != alias_map.end()) {
			// found entry!
			auto index = alias_entry->second;
			if (index >= node.bound_column_count) {
				throw BinderException("Column \"%s\" referenced that exists in the SELECT clause - but this column "
				                      "cannot be referenced before it is defined",
				                      colref.column_names[0]);
			}
			if (node.select_list[index]->IsVolatile()) {
				throw BinderException("Alias \"%s\" referenced in a SELECT clause - but the expression has side "
				                      "effects. This is not yet supported.",
				                      colref.column_names[0]);
			}
			if (node.select_list[index]->HasSubquery()) {
				throw BinderException("Alias \"%s\" referenced in a SELECT clause - but the expression has a subquery."
				                      " This is not yet supported.",
				                      colref.column_names[0]);
			}
			auto copied_expression = node.original_expressions[index]->Copy();
			result = BindExpression(copied_expression, depth, false);
			return result;
		}
	}
	// entry was not found in the alias map: return the original error
	return result;
}

BindResult BaseSelectBinder::BindGroupingFunction(OperatorExpression &op, idx_t depth) {
	if (op.children.empty()) {
		throw InternalException("GROUPING requires at least one child");
	}
	if (node.groups.group_expressions.empty()) {
		return BindResult(BinderException(op, "GROUPING statement cannot be used without groups"));
	}
	if (op.children.size() >= 64) {
		return BindResult(BinderException(op, "GROUPING statement cannot have more than 64 groups"));
	}
	vector<idx_t> group_indexes;
	group_indexes.reserve(op.children.size());
	for (auto &child : op.children) {
		ExpressionBinder::QualifyColumnNames(binder, child);
		auto idx = TryBindGroup(*child);
		if (idx == DConstants::INVALID_INDEX) {
			return BindResult(BinderException(op, "GROUPING child \"%s\" must be a grouping column", child->GetName()));
		}
		group_indexes.push_back(idx);
	}
	auto col_idx = node.grouping_functions.size();
	node.grouping_functions.push_back(std::move(group_indexes));
	return BindResult(make_uniq<BoundColumnRefExpression>(op.GetName(), LogicalType::BIGINT,
	                                                      ColumnBinding(node.groupings_index, col_idx), depth));
}

BindResult BaseSelectBinder::BindGroup(ParsedExpression &expr, idx_t depth, idx_t group_index) {
	auto it = info.collated_groups.find(group_index);
	if (it != info.collated_groups.end()) {
		// This is an implicitly collated group, so we need to refer to the first() aggregate
		const auto &aggr_index = it->second;
		return BindResult(make_uniq<BoundColumnRefExpression>(expr.GetName(), node.aggregates[aggr_index]->return_type,
		                                                      ColumnBinding(node.aggregate_index, aggr_index), depth));
	} else {
		auto &group = node.groups.group_expressions[group_index];
		return BindResult(make_uniq<BoundColumnRefExpression>(expr.GetName(), group->return_type,
		                                                      ColumnBinding(node.group_index, group_index), depth));
	}
}

bool BaseSelectBinder::QualifyColumnAlias(const ColumnRefExpression &colref) {
	if (!colref.IsQualified()) {
		return alias_map.find(colref.column_names[0]) != alias_map.end();
	}
	return false;
}

} // namespace duckdb
