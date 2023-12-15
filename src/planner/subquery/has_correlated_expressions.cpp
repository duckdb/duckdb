#include "duckdb/planner/subquery/has_correlated_expressions.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"

#include <algorithm>

namespace duckdb {

HasCorrelatedExpressions::HasCorrelatedExpressions(const vector<CorrelatedColumnInfo> &correlated, bool lateral,
                                                   idx_t lateral_depth)
    : has_correlated_expressions(false), lateral(lateral), correlated_columns(correlated),
      lateral_depth(lateral_depth) {
}

void HasCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> HasCorrelatedExpressions::VisitReplace(BoundColumnRefExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	// Indicates local correlations (all correlations within a child) for the root
	if (expr.depth <= lateral_depth) {
		return nullptr;
	}

	// Should never happen
	if (expr.depth > 1 + lateral_depth) {
		if (lateral) {
			throw BinderException("Invalid lateral depth encountered for an expression");
		}
		throw InternalException("Expression with depth > 1 detected in non-lateral join");
	}
	// Note: This is added, since we only want to set has_correlated_expressions to true when the
	// BoundSubqueryExpression has the same bindings as one of the correlated_columns from the left hand side
	// (correlated_columns is the correlated_columns from left hand side)
	bool found_match = false;
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		if (correlated_columns[i].binding == expr.binding) {
			found_match = true;
			break;
		}
	}
	// correlated column reference
	D_ASSERT(expr.depth == lateral_depth + 1);
	has_correlated_expressions = found_match;
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
