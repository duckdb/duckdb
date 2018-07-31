
#include "parser/sql_node_visitor.hpp"

#include "parser/expression/expression_list.hpp"

using namespace duckdb;
using namespace std;

void SQLNodeVisitor::Visit(SelectStatement &statement) {}

void SQLNodeVisitor::Visit(AggregateExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(BaseTableRefExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(CastExpression &expr) {
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
void SQLNodeVisitor::Visit(CrossProductExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(FunctionExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(GroupRefExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(JoinExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(OperatorExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(SubqueryExpression &expr) {
	expr.AcceptChildren(this);
}
void SQLNodeVisitor::Visit(TableRefExpression &expr) {
	expr.AcceptChildren(this);
}