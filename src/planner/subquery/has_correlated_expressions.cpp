#include "planner/subquery/has_correlated_expressions.hpp"
#include "parser/expression/bound_columnref_expression.hpp"
#include "parser/expression/bound_subquery_expression.hpp"

using namespace duckdb;
using namespace std;

HasCorrelatedExpressions::HasCorrelatedExpressions(const vector<CorrelatedColumnInfo> &correlated)
	: has_correlated_expressions(false), correlated_columns(correlated) {
}

void HasCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	//! The HasCorrelatedExpressions does not recursively visit logical operators, it only visits the current one
	VisitOperatorExpressions(op);
}

void HasCorrelatedExpressions::Visit(BoundColumnRefExpression &expr) {
	if (expr.depth == 0) {
		return;
	}
	// correlated column reference
	assert(expr.depth == 1);
	has_correlated_expressions = true;
}

void HasCorrelatedExpressions::Visit(BoundSubqueryExpression &expr) {
	if (!expr.IsCorrelated()) {
		return;
	}
	// check if the subquery contains any of the correlated expressions that we are concerned about in this node
	for (size_t i = 0; i < correlated_columns.size(); i++) {
		if (std::find(expr.binder->correlated_columns.begin(), expr.binder->correlated_columns.end(),
						correlated_columns[i]) != expr.binder->correlated_columns.end()) {
			has_correlated_expressions = true;
			break;
		}
	}
}
