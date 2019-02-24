#include "planner/subquery/rewrite_correlated_expressions.hpp"

#include "parser/expression/bound_columnref_expression.hpp"
#include "parser/expression/bound_subquery_expression.hpp"
#include "parser/expression/case_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/operator_expression.hpp"

using namespace duckdb;
using namespace std;

RewriteCorrelatedExpressions::RewriteCorrelatedExpressions(ColumnBinding base_binding,
                                                           column_binding_map_t<size_t> &correlated_map)
    : base_binding(base_binding), correlated_map(correlated_map) {
}

void RewriteCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	VisitOperatorExpressions(op);
}
void RewriteCorrelatedExpressions::Visit(BoundColumnRefExpression &expr) {
	if (expr.depth == 0) {
		return;
	}
	// correlated column reference
	// replace with the entry referring to the duplicate eliminated scan
	assert(expr.depth == 1);
	auto entry = correlated_map.find(expr.binding);
	assert(entry != correlated_map.end());
	expr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
	expr.depth = 0;
}

void RewriteCorrelatedExpressions::Visit(BoundSubqueryExpression &expr) {
	if (!expr.IsCorrelated()) {
		return;
	}
	// subquery detected within this subquery
	// recursively rewrite it using the RewriteCorrelatedRecursive class
	RewriteCorrelatedRecursive rewrite(expr, base_binding, correlated_map);
	rewrite.RewriteCorrelatedSubquery(expr);
}

RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedRecursive(
    BoundSubqueryExpression &parent, ColumnBinding base_binding, column_binding_map_t<size_t> &correlated_map)
    : parent(parent), base_binding(base_binding), correlated_map(correlated_map) {
}

void RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedSubquery(
    BoundSubqueryExpression &expr) {
	// rewrite the binding in the correlated list of the subquery)
	for (auto &corr : expr.binder->correlated_columns) {
		auto entry = correlated_map.find(corr.binding);
		if (entry != correlated_map.end()) {
			corr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
		}
	}
	// now rewrite any correlated BoundColumnRef expressions inside the subquery
	auto &subquery = (SubqueryExpression &)*expr.subquery;
	subquery.subquery->EnumerateChildren([&](Expression *child) { RewriteCorrelatedExpressions(child); });
}

void RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedExpressions(Expression *child) {
	if (child->type == ExpressionType::BOUND_COLUMN_REF) {
		// bound column reference
		auto &bound_colref = (BoundColumnRefExpression &)*child;
		if (bound_colref.depth == 0) {
			// not a correlated column, ignore
			return;
		}
		// correlated column
		// check the correlated map
		auto entry = correlated_map.find(bound_colref.binding);
		if (entry != correlated_map.end()) {
			// we found the column in the correlated map!
			// update the binding and reduce the depth by 1
			bound_colref.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
			bound_colref.depth--;
		}
	} else if (child->type == ExpressionType::SUBQUERY) {
		// we encountered another subquery: rewrite recursively
		assert(child->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY);
		auto &bound_subquery = (BoundSubqueryExpression &)*child;
		RewriteCorrelatedRecursive rewrite(bound_subquery, base_binding, correlated_map);
		rewrite.RewriteCorrelatedSubquery(bound_subquery);
	}
}

RewriteCountAggregates::RewriteCountAggregates(column_binding_map_t<size_t> &replacement_map)
    : replacement_map(replacement_map) {
}

unique_ptr<Expression> RewriteCountAggregates::VisitReplace(BoundColumnRefExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	auto entry = replacement_map.find(expr.binding);
	if (entry != replacement_map.end()) {
		// reference to a COUNT(*) aggregate
		// replace this with CASE WHEN COUNT(*) IS NULL THEN 0 ELSE COUNT(*) END
		auto case_expr = make_unique<CaseExpression>();
		auto is_null = make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, TypeId::BOOLEAN, expr.Copy());
		case_expr->check = move(is_null);
		case_expr->result_if_true = make_unique<ConstantExpression>(Value::Numeric(expr.return_type, 0));
		case_expr->result_if_false = move(*expr_ptr);
		case_expr->return_type = expr.return_type;
		return move(case_expr);
	}
	return nullptr;
}
