#include "parser/sql_node_visitor.hpp"

#include "parser/constraints/list.hpp"
#include "parser/expression/list.hpp"
#include "parser/tableref/list.hpp"

using namespace duckdb;
using namespace std;

void SQLNodeVisitor::VisitExpression(Expression *expr_ptr) {
	auto &expr = *expr_ptr;
	auto expr_class = expr.GetExpressionClass();
	switch (expr_class) {
	case ExpressionClass::AGGREGATE:
		Visit((AggregateExpression &)expr);
		break;
	case ExpressionClass::BOUND_REF:
		Visit((BoundExpression &)expr);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		Visit((BoundFunctionExpression &)expr);
		break;
	case ExpressionClass::BOUND_COLUMN_REF:
		Visit((BoundColumnRefExpression &)expr);
		break;
	case ExpressionClass::BOUND_SUBQUERY:
		Visit((BoundSubqueryExpression &)expr);
		break;
	case ExpressionClass::CASE:
		Visit((CaseExpression &)expr);
		break;
	case ExpressionClass::CAST:
		Visit((CastExpression &)expr);
		break;
	case ExpressionClass::COLUMN_REF:
		Visit((ColumnRefExpression &)expr);
		break;
	case ExpressionClass::COMPARISON:
		Visit((ComparisonExpression &)expr);
		break;
	case ExpressionClass::CONJUNCTION:
		Visit((ConjunctionExpression &)expr);
		break;
	case ExpressionClass::CONSTANT:
		Visit((ConstantExpression &)expr);
		break;
	case ExpressionClass::DEFAULT:
		Visit((DefaultExpression &)expr);
		break;
	case ExpressionClass::FUNCTION:
		Visit((FunctionExpression &)expr);
		break;
	case ExpressionClass::OPERATOR:
		Visit((OperatorExpression &)expr);
		break;
	case ExpressionClass::PARAMETER:
		Visit((ParameterExpression &)expr);
		break;
	case ExpressionClass::STAR:
		Visit((StarExpression &)expr);
		break;
	case ExpressionClass::SUBQUERY:
		Visit((SubqueryExpression &)expr);
		break;
	case ExpressionClass::WINDOW:
		Visit((WindowExpression &)expr);
		break;
	case ExpressionClass::COMMON_SUBEXPRESSION:
		Visit((CommonSubExpression &)expr);
		break;
	default:
		throw Exception("Unsupported expression class");
		break;
	}
	// visit the children of this node
	VisitExpressionChildren(expr);
}

void SQLNodeVisitor::VisitExpression(unique_ptr<Expression> *expr_ptr) {
	auto expr_class = (*expr_ptr)->GetExpressionClass();
	unique_ptr<Expression> retval;
	switch (expr_class) {
	case ExpressionClass::AGGREGATE:
		retval = VisitReplace((AggregateExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::BOUND_REF:
		retval = VisitReplace((BoundExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		retval = VisitReplace((BoundFunctionExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::BOUND_COLUMN_REF:
		retval = VisitReplace((BoundColumnRefExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::BOUND_SUBQUERY:
		retval = VisitReplace((BoundSubqueryExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::CASE:
		retval = VisitReplace((CaseExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::CAST:
		retval = VisitReplace((CastExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::COLUMN_REF:
		retval = VisitReplace((ColumnRefExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::COMPARISON:
		retval = VisitReplace((ComparisonExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::CONJUNCTION:
		retval = VisitReplace((ConjunctionExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::CONSTANT:
		retval = VisitReplace((ConstantExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::DEFAULT:
		retval = VisitReplace((DefaultExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::FUNCTION:
		retval = VisitReplace((FunctionExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::OPERATOR:
		retval = VisitReplace((OperatorExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::PARAMETER:
		retval = VisitReplace((ParameterExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::STAR:
		retval = VisitReplace((StarExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::SUBQUERY:
		retval = VisitReplace((SubqueryExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::WINDOW:
		retval = VisitReplace((WindowExpression &)**expr_ptr, expr_ptr);
		break;
	case ExpressionClass::COMMON_SUBEXPRESSION:
		retval = VisitReplace((CommonSubExpression &)**expr_ptr, expr_ptr);
		break;
	default:
		throw Exception("Unsupported expression class");
	}
	if (retval) {
		*expr_ptr = move(retval);
	}
	// visit the children of this node
	VisitExpressionChildren(**expr_ptr);
}

void SQLNodeVisitor::VisitExpressionChildren(Expression &expr) {
	expr.EnumerateChildren([&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
		VisitExpression(&expr);
		return expr;
	});
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(AggregateExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(BoundExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(CaseExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(CastExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(CommonSubExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(ColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(ComparisonExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(ConjunctionExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(ConstantExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(DefaultExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(FunctionExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(OperatorExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(ParameterExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(StarExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(SubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(WindowExpression &expr, unique_ptr<Expression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

void SQLNodeVisitor::Visit(CheckConstraint &con) {
	VisitExpression(&con.expression);
}

unique_ptr<TableRef> SQLNodeVisitor::Visit(CrossProductRef &expr) {
	AcceptChild(&expr.left);
	AcceptChild(&expr.right);
	return nullptr;
}

unique_ptr<TableRef> SQLNodeVisitor::Visit(JoinRef &expr) {
	AcceptChild(&expr.left);
	AcceptChild(&expr.right);
	return nullptr;
}
