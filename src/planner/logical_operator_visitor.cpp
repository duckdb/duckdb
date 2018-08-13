
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
void LogicalOperatorVisitor::Visit(LogicalJoin &op) {
	op.AcceptChildren(this);
	op.condition->Accept(this);
}
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
void LogicalOperatorVisitor::Visit(LogicalCopy &op) { op.AcceptChildren(this); }
