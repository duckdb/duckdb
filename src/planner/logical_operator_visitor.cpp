
#include "planner/logical_operator_visitor.hpp"

#include "planner/operator/logical_list.hpp"

using namespace duckdb;
using namespace std;

void LogicalOperatorVisitor::Visit(LogicalAggregate &op) {
	op.AcceptChildren(this);
	for (auto &exp : op.select_list) {
		exp->Accept(this);
	}
}

void LogicalOperatorVisitor::Visit(LogicalCrossProduct &op) {
	op.AcceptChildren(this);
}

void LogicalOperatorVisitor::Visit(LogicalDistinct &op) {
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalFilter &op) {
	op.AcceptChildren(this);
	for (auto &exp : op.expressions) {
		exp->Accept(this);
	}
}
void LogicalOperatorVisitor::Visit(LogicalGet &op) { op.AcceptChildren(this); }
void LogicalOperatorVisitor::Visit(LogicalLimit &op) {
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalOrder &op) {
	op.AcceptChildren(this);
	for (auto &exp : op.description.orders) {
		exp.expression->Accept(this);
	}
}
void LogicalOperatorVisitor::Visit(LogicalProjection &op) {
	op.AcceptChildren(this);
	for (auto &exp : op.select_list) {
		exp->Accept(this);
	}
}
void LogicalOperatorVisitor::Visit(LogicalInsert &op) {
	op.AcceptChildren(this);
	for (auto &exp : op.value_list) {
		exp->Accept(this);
	}
}
