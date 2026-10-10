#include "duckdb/optimizer/in_clause_rewriter.hpp"
#include "duckdb/execution/expression_executor/in_list_lookup.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

//! Large constant lists are kept as IN: the executor probes a lookup or compares them one by one
static bool KeepInExpression(const BoundOperatorExpression &expr) {
	auto &children = expr.GetChildren();
	if (children.size() <= InListLookup::MIN_VALUE_COUNT) {
		return false;
	}
	for (idx_t child_idx = 1; child_idx < children.size(); child_idx++) {
		if (!children[child_idx]->IsFoldable()) {
			return false;
		}
	}
	return true;
}

bool InClauseRewriter::ContainsLargeConstantInClause(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    (expr.GetExpressionType() == ExpressionType::COMPARE_IN ||
	     expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) &&
	    KeepInExpression(expr.Cast<BoundOperatorExpression>())) {
		return true;
	}
	bool result = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!result && ContainsLargeConstantInClause(child)) {
			result = true;
		}
	});
	return result;
}

unique_ptr<LogicalOperator> InClauseRewriter::Rewrite(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
		VisitOperatorExpressions(*op);
		break;
	default:
		break;
	}

	for (auto &child : op->children) {
		child = Rewrite(std::move(child));
	}
	return op;
}

unique_ptr<Expression> InClauseRewriter::VisitReplace(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr) {
	if (expr.GetExpressionType() != ExpressionType::COMPARE_IN &&
	    expr.GetExpressionType() != ExpressionType::COMPARE_NOT_IN) {
		return nullptr;
	}
	VisitExpressionChildren(expr);
	bool is_regular_in = expr.GetExpressionType() == ExpressionType::COMPARE_IN;
	if (expr.GetChildrenMutable().size() == 2) {
		// only one child
		// IN: turn into X = 1
		// NOT IN: turn into X <> 1
		return BoundComparisonExpression::Create(
		    is_regular_in ? ExpressionType::COMPARE_EQUAL : ExpressionType::COMPARE_NOTEQUAL,
		    std::move(expr.GetChildrenMutable()[0]), std::move(expr.GetChildrenMutable()[1]));
	}
	if (KeepInExpression(expr)) {
		return nullptr;
	}
	// low amount of children or not all scalar
	// IN: turn into (X = 1 OR X = 2 OR X = 3...)
	// NOT IN: turn into (X <> 1 AND X <> 2 AND X <> 3 ...)
	auto conjunction = make_uniq<BoundConjunctionExpression>(is_regular_in ? ExpressionType::CONJUNCTION_OR
	                                                                       : ExpressionType::CONJUNCTION_AND);
	for (idx_t i = 1; i < expr.GetChildrenMutable().size(); i++) {
		conjunction->GetChildrenMutable().push_back(BoundComparisonExpression::Create(
		    is_regular_in ? ExpressionType::COMPARE_EQUAL : ExpressionType::COMPARE_NOTEQUAL,
		    expr.GetChildrenMutable()[0]->Copy(), std::move(expr.GetChildrenMutable()[i])));
	}
	return std::move(conjunction);
}

} // namespace duckdb
