
#include "planner/logical_operator_visitor.hpp"

#include "planner/operator/logical_aggregate.hpp"
#include "planner/operator/logical_distinct.hpp"
#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_get.hpp"
#include "planner/operator/logical_insert.hpp"
#include "planner/operator/logical_limit.hpp"
#include "planner/operator/logical_list.hpp"
#include "planner/operator/logical_order.hpp"
#include "planner/operator/logical_projection.hpp"

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

void LogicalOperatorVisitor::Visit(LogicalDistinct &op) {
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
	for(auto &cond : op.conditions) {
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
	for (auto &exp : op.expressions) {
		exp->Accept(this);
	}
}

void LogicalOperatorVisitor::Visit(LogicalCopy &op) {
	VisitOperator(op);
}
