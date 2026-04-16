#include "duckdb/planner/subquery/has_correlated_expressions.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"

#include <algorithm>

namespace duckdb {

HasCorrelatedExpressions::HasCorrelatedExpressions(const CorrelatedColumns &correlated)
    : has_correlated_expressions(false), correlated_columns(correlated) {
}

bool HasCorrelatedExpressions::Detect(LogicalOperator &op, const CorrelatedColumns &correlated) {
	HasCorrelatedExpressions visitor(correlated);
	visitor.VisitOperator(op);
	return visitor.has_correlated_expressions;
}

void HasCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> HasCorrelatedExpressions::VisitReplace(BoundColumnRefExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	if (expr.depth == 0) {
		return nullptr;
	}
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		if (correlated_columns[i].binding == expr.binding) {
			has_correlated_expressions = true;
			break;
		}
	}
	return nullptr;
}

unique_ptr<Expression> HasCorrelatedExpressions::VisitReplace(BoundSubqueryExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	if (!expr.IsCorrelated()) {
		return nullptr;
	}
	// check if the subquery contains any of the correlated expressions that we are concerned about in this node
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		if (std::find(expr.binder->correlated_columns.begin(), expr.binder->correlated_columns.end(),
		              correlated_columns[i]) != expr.binder->correlated_columns.end()) {
			has_correlated_expressions = true;
			break;
		}
	}
	return nullptr;
}

} // namespace duckdb
