
#include "parser/sql_node_visitor.hpp"

#include "parser/constraints/list.hpp"
#include "parser/expression/list.hpp"
#include "parser/tableref/tableref_list.hpp"

using namespace duckdb;
using namespace std;

void SQLNodeVisitor::Visit(SelectStatement &statement) {}

void SQLNodeVisitor::Visit(AggregateExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(CaseExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(CastExpression &expr) { expr.AcceptChildren(this); }
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
void SQLNodeVisitor::Visit(GroupRefExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(OperatorExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(SubqueryExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(StarExpression &expr) { expr.AcceptChildren(this); }

void SQLNodeVisitor::Visit(CheckConstraint &con) {
	con.expression->Accept(this);
}
void SQLNodeVisitor::Visit(NotNullConstraint &con) {}
void SQLNodeVisitor::Visit(ParsedConstraint &con) {}

void SQLNodeVisitor::Visit(BaseTableRef &expr) {}

void SQLNodeVisitor::Visit(CrossProductRef &expr) {
	expr.left->Accept(this);
	expr.right->Accept(this);
}

void SQLNodeVisitor::Visit(JoinRef &expr) {
	expr.left->Accept(this);
	expr.right->Accept(this);
}

void SQLNodeVisitor::Visit(SubqueryRef &expr) {}