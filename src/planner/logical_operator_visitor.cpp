
#include "planner/logical_operator_visitor.hpp"

#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalOperatorVisitor::VisitOperator(LogicalOperator &op) {
	op.AcceptChildren(this);
}

void LogicalOperatorVisitor::Visit(LogicalAggregate &op) {
	VisitOperator(op);
	for (auto &exp : op.groups) {
		exp->Accept(this);
	}
	for (auto &exp : op.expressions) {
		exp->Accept(this);
	}
}

void LogicalOperatorVisitor::Visit(LogicalCrossProduct &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalSubquery &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalTableFunction &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalDelete &op) {
	VisitOperator(op);
	for (auto &exp : op.expressions) {
		exp->Accept(this);
	}
}

void LogicalOperatorVisitor::Visit(LogicalUpdate &op) {
	VisitOperator(op);
	for (auto &exp : op.expressions) {
		exp->Accept(this);
	}
}

void LogicalOperatorVisitor::Visit(LogicalUnion &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalFilter &op) {
	VisitOperator(op);
	for (auto &exp : op.expressions) {
		exp->Accept(this);
	}
}

void LogicalOperatorVisitor::Visit(LogicalGet &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalJoin &op) {
	for (auto &cond : op.conditions) {
		cond.left->Accept(this);
		cond.right->Accept(this);
	}
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalLimit &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalOrder &op) {
	VisitOperator(op);
	for (auto &exp : op.description.orders) {
		exp.expression->Accept(this);
	}
}

void LogicalOperatorVisitor::Visit(LogicalProjection &op) {
	VisitOperator(op);
	for (auto &exp : op.expressions) {
		exp->Accept(this);
	}
}

void LogicalOperatorVisitor::Visit(LogicalInsert &op) {
	VisitOperator(op);

	for (auto &list : op.insert_values) {
		for (auto &exp : list) {
			exp->Accept(this);
		}
	}
}

void LogicalOperatorVisitor::Visit(LogicalCreate &op) {
	VisitOperator(op);
	// FIXME: visit expressions in constraints
}

void LogicalOperatorVisitor::Visit(LogicalCopy &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalExplain &op) {
	VisitOperator(op);
}
