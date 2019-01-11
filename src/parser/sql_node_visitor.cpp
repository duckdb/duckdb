#include "parser/sql_node_visitor.hpp"

#include "parser/constraints/list.hpp"
#include "parser/expression/list.hpp"
#include "parser/tableref/list.hpp"

using namespace duckdb;
using namespace std;

void SQLNodeVisitor::Visit(AggregateExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(WindowExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(BoundExpression &expr) {
}
void SQLNodeVisitor::Visit(CaseExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(CastExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(CommonSubExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(ColumnRefExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(ComparisonExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(ConjunctionExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(ConstantExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(DefaultExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(FunctionExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(OperatorExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(SubqueryExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(StarExpression &expr) {
	expr.AcceptChildren(this);
}

void SQLNodeVisitor::Visit(CheckConstraint &con) {
	con.expression->Accept(this);
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
