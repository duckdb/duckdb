#include "parser/sql_node_visitor.hpp"

#include "parser/constraints/list.hpp"
#include "parser/expression/list.hpp"
#include "parser/tableref/list.hpp"

using namespace duckdb;
using namespace std;

void SQLNodeVisitor::VisitExpression(ParsedExpression *expr_ptr) {
	auto &expr = *expr_ptr;
	auto expr_class = expr.GetExpressionClass();
	switch (expr_class) {
	case ExpressionClass::AGGREGATE:
		Visit((AggregateExpression &)expr);
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
	default:
		assert(expr_class == ExpressionClass::WINDOW);
		Visit((WindowExpression &)expr);
		break;
	}
	// visit the children of this node
	VisitExpressionChildren(expr);
}

void SQLNodeVisitor::VisitExpression(unique_ptr<ParsedExpression> *expr_ptr) {
	auto expr_class = (*expr_ptr)->GetExpressionClass();
	unique_ptr<ParsedExpression> retval;
	switch (expr_class) {
	case ExpressionClass::AGGREGATE:
		retval = VisitReplace((AggregateExpression &)**expr_ptr, expr_ptr);
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
	default:
		assert(expr_class == ExpressionClass::WINDOW);
		retval = VisitReplace((WindowExpression &)**expr_ptr, expr_ptr);
		break;
	}
	if (retval) {
		*expr_ptr = move(retval);
	} else {
		// visit the children of this node
		VisitExpressionChildren(**expr_ptr);
	}
}

void SQLNodeVisitor::VisitExpressionChildren(ParsedExpression &expr) {
	expr.EnumerateChildren([&](unique_ptr<ParsedExpression> expr) -> unique_ptr<ParsedExpression> {
		VisitExpression(&expr);
		return expr;
	});
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(AggregateExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(CaseExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(CastExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(ColumnRefExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(ComparisonExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(ConjunctionExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(ConstantExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(DefaultExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(FunctionExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(OperatorExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(ParameterExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(StarExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(SubqueryExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
	Visit(expr);
	return nullptr;
}

unique_ptr<ParsedExpression> SQLNodeVisitor::VisitReplace(WindowExpression &expr, unique_ptr<ParsedExpression> *expr_ptr) {
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
