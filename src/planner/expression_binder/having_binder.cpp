#include "duckdb/planner/expression_binder/having_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

HavingBinder::HavingBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info, unordered_map<string, idx_t> &alias_map)
    : SelectBinder(binder, context, node, info), alias_map(alias_map), in_alias(false) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult HavingBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr, depth);
	if (group_index != INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("HAVING clause cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		if (!in_alias) {
			auto &colref = (ColumnRefExpression &) expr;
			if (colref.table_name.empty()) {
				auto alias_entry = alias_map.find(colref.column_name);
				if (alias_entry != alias_map.end()) {
					// found an alias: bind the alias expression
					auto expression = node.original_expressions[alias_entry->second]->Copy();
					in_alias = true;
					auto result = BindExpression(&expression, depth, root_expression);
					in_alias = false;
					return result;
				}
			}
		}
		return BindResult(StringUtil::Format(
		    "column %s must appear in the GROUP BY clause or be used in an aggregate function", expr.ToString()));
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

} // namespace duckdb
