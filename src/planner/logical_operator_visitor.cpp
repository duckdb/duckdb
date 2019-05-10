#include "planner/logical_operator_visitor.hpp"

#include "planner/expression/list.hpp"
#include "planner/expression_iterator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalOperatorVisitor::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

void LogicalOperatorVisitor::VisitOperatorChildren(LogicalOperator &op) {
	for (auto &child : op.children) {
		VisitOperator(*child);
	}
}

void LogicalOperatorVisitor::VisitOperatorExpressions(LogicalOperator &op) {
	for (uint64_t i = 0, child_count = op.ExpressionCount(); i < child_count; i++) {
		op.ReplaceExpression(
		    [&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
			    VisitExpression(&child);
			    return child;
		    },
		    i);
	}
}

void LogicalOperatorVisitor::VisitExpression(unique_ptr<Expression> *expression) {
	auto &expr = **expression;
	unique_ptr<Expression> result;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE:
		result = VisitReplace((BoundAggregateExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_CASE:
		result = VisitReplace((BoundCaseExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_CAST:
		result = VisitReplace((BoundCastExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_COLUMN_REF:
		result = VisitReplace((BoundColumnRefExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		result = VisitReplace((BoundComparisonExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		result = VisitReplace((BoundConjunctionExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		result = VisitReplace((BoundConstantExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		result = VisitReplace((BoundFunctionExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_SUBQUERY:
		result = VisitReplace((BoundSubqueryExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		result = VisitReplace((BoundOperatorExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		result = VisitReplace((BoundParameterExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_REF:
		result = VisitReplace((BoundReferenceExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_DEFAULT:
		result = VisitReplace((BoundDefaultExpression &)expr, expression);
		break;
	case ExpressionClass::COMMON_SUBEXPRESSION:
		result = VisitReplace((CommonSubExpression &)expr, expression);
		break;
	default:
		assert(expr.GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		result = VisitReplace((BoundWindowExpression &)expr, expression);
		break;
	}
	if (result) {
		*expression = move(result);
	} else {
		// visit the children of this node
		VisitExpressionChildren(expr);
	}
}

void LogicalOperatorVisitor::VisitExpressionChildren(Expression &expr) {
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
		VisitExpression(&expr);
		return expr;
	});
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundAggregateExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundCaseExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundCastExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundColumnRefExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundComparisonExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundConjunctionExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundConstantExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundDefaultExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundFunctionExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundOperatorExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundParameterExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundReferenceExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundSubqueryExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundWindowExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(CommonSubExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}
