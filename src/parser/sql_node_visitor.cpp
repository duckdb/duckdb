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
	case ExpressionClass::ALIAS_REF:
		assert(0);
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
	case ExpressionClass::BOUND_REF:
		Visit((BoundExpression &)expr);
		break;
	default:
		assert(0);
		break;
	}
	// visit the children of this node
	VisitExpressionChildren(expr);
}

void SQLNodeVisitor::VisitExpression(unique_ptr<Expression> *expr_ptr) {
	auto &expr = **expr_ptr;
	auto expr_class = expr.GetExpressionClass();
	unique_ptr<Expression> retval;
	switch (expr_class) {
	case ExpressionClass::AGGREGATE:
		retval = VisitReplace((AggregateExpression &)expr);
		break;
	case ExpressionClass::ALIAS_REF:
		assert(0);
		break;
	case ExpressionClass::CASE:
		retval = VisitReplace((CaseExpression &)expr);
		break;
	case ExpressionClass::CAST:
		retval = VisitReplace((CastExpression &)expr);
		break;
	case ExpressionClass::COLUMN_REF:
		retval = VisitReplace((ColumnRefExpression &)expr);
		break;
	case ExpressionClass::COMPARISON:
		retval = VisitReplace((ComparisonExpression &)expr);
		break;
	case ExpressionClass::CONJUNCTION:
		retval = VisitReplace((ConjunctionExpression &)expr);
		break;
	case ExpressionClass::CONSTANT:
		retval = VisitReplace((ConstantExpression &)expr);
		break;
	case ExpressionClass::DEFAULT:
		retval = VisitReplace((DefaultExpression &)expr);
		break;
	case ExpressionClass::FUNCTION:
		retval = VisitReplace((FunctionExpression &)expr);
		break;
	case ExpressionClass::OPERATOR:
		retval = VisitReplace((OperatorExpression &)expr);
		break;
	case ExpressionClass::STAR:
		retval = VisitReplace((StarExpression &)expr);
		break;
	case ExpressionClass::SUBQUERY:
		retval = VisitReplace((SubqueryExpression &)expr);
		break;
	case ExpressionClass::WINDOW:
		retval = VisitReplace((WindowExpression &)expr);
		break;
	case ExpressionClass::COMMON_SUBEXPRESSION:
		retval = VisitReplace((CommonSubExpression &)expr);
		break;
	case ExpressionClass::BOUND_REF:
		assert(expr_class == ExpressionClass::BOUND_REF);
		retval = VisitReplace((BoundExpression &)expr);
		break;
	default:
		assert(0);
		break;
	}
	if (retval) {
		*expr_ptr = move(retval);
	}
	// visit the children of this node
	VisitExpressionChildren(expr);
}

void SQLNodeVisitor::VisitExpressionChildren(Expression &expr) {
	expr.EnumerateChildren([&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
		VisitExpression(&expr);
		return expr;
	});
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(AggregateExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(BoundExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(CaseExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(CastExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(CommonSubExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(ColumnRefExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(ComparisonExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(ConjunctionExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(ConstantExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(DefaultExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(FunctionExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(OperatorExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(StarExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(SubqueryExpression &expr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<Expression> SQLNodeVisitor::VisitReplace(WindowExpression &expr) {
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
