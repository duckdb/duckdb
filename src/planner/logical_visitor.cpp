
#include "planner/logical_visitor.hpp"

#include "planner/operator/logical_list.hpp"

using namespace duckdb;
using namespace std;

void LogicalOperatorVisitor::Visit(LogicalAggregate &op) {
	for (auto &exp : op.select_list) {
		exp->Accept(this);
	}
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalDistinct &op) {
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalFilter &op) {
	for (auto &exp : op.expressions) {
		exp->Accept(this);
	}
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalGet &op) { op.AcceptChildren(this); }
void LogicalOperatorVisitor::Visit(LogicalLimit &op) {
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalOrder &op) {
	for (auto &exp : op.description.orders) {
		exp.expression->Accept(this);
	}
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalProjection &op) {
	for (auto &exp : op.select_list) {
		exp->Accept(this);
	}
	op.AcceptChildren(this);
}
void LogicalOperatorVisitor::Visit(LogicalInsert &op) {
	for (auto &exp : op.value_list) {
		exp->Accept(this);
	}
	op.AcceptChildren(this);
}
