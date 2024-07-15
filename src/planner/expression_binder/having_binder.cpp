#include "duckdb/planner/expression_binder/having_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

HavingBinder::HavingBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
                           AggregateHandling aggregate_handling)
    : BaseSelectBinder(binder, context, node, info), column_alias_binder(node.bind_state),
      aggregate_handling(aggregate_handling) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult HavingBinder::BindLambdaReference(LambdaRefExpression &expr, idx_t depth) {
	D_ASSERT(lambda_bindings && expr.lambda_idx < lambda_bindings->size());
	auto &lambda_ref = expr.Cast<LambdaRefExpression>();
	return (*lambda_bindings)[expr.lambda_idx].Bind(lambda_ref, depth);
}

BindResult HavingBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {

	// Keep the original column name to return a meaningful error message.
	auto col_ref = expr_ptr->Cast<ColumnRefExpression>();
	const auto &column_name = col_ref.GetColumnName();

	// Try binding as a lambda parameter
	if (!col_ref.IsQualified()) {
		auto lambda_ref = LambdaRefExpression::FindMatchingBinding(lambda_bindings, col_ref.GetName());
		if (lambda_ref) {
			return BindLambdaReference(lambda_ref->Cast<LambdaRefExpression>(), depth);
		}
		// column was not found - check if it is a SQL value function
		auto value_function = GetSQLValueFunction(col_ref.GetColumnName());
		if (value_function) {
			return BindExpression(value_function, depth);
		}
	}

	// Bind the alias.
	BindResult alias_result;
	auto found_alias = column_alias_binder.BindAlias(*this, expr_ptr, depth, root_expression, alias_result);
	if (found_alias) {
		if (depth > 0) {
			throw BinderException("Having clause cannot reference alias \"%s\" in correlated subquery", column_name);
		}
		return alias_result;
	}

	if (aggregate_handling != AggregateHandling::FORCE_AGGREGATES) {
		return BindResult(StringUtil::Format(
		    "column %s must appear in the GROUP BY clause or be used in an aggregate function", column_name));
	}

	if (depth > 0) {
		throw BinderException("Having clause cannot reference column \"%s\" in correlated subquery and group by all",
		                      column_name);
	}

	auto expr = duckdb::BaseSelectBinder::BindColumnRef(expr_ptr, depth, root_expression);
	if (expr.HasError()) {
		return expr;
	}

	// Return a GROUP BY column reference expression.
	auto return_type = expr.expression->return_type;
	auto column_binding = ColumnBinding(node.group_index, node.groups.group_expressions.size());
	auto group_ref = make_uniq<BoundColumnRefExpression>(return_type, column_binding);
	node.groups.group_expressions.push_back(std::move(expr.expression));
	return BindResult(std::move(group_ref));
}

BindResult HavingBinder::BindWindow(WindowExpression &expr, idx_t depth) {
	return BindResult(BinderException::Unsupported(expr, "HAVING clause cannot contain window functions!"));
}

} // namespace duckdb
