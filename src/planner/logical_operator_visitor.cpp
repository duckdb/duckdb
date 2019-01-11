#include "planner/logical_operator_visitor.hpp"

#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalOperatorVisitor::VisitOperator(LogicalOperator &op) {
	op.AcceptChildren(this);
	for(size_t i = 0, child_count = op.ExpressionCount(); i < child_count; i++) {
		op.ReplaceExpression([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
			VisitExpression(&child);
			return child;
		}, i);
	}
}

void LogicalOperatorVisitor::Visit(LogicalAggregate &op) {
	VisitOperator(op);
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

void LogicalOperatorVisitor::Visit(LogicalPruneColumns &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalDelete &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalUpdate &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalUnion &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalExcept &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalIntersect &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalFilter &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalGet &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalJoin &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalLimit &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalOrder &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalProjection &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalInsert &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalCreate &op) {
	VisitOperator(op);
	// FIXME: visit expressions in constraints
}

void LogicalOperatorVisitor::Visit(LogicalCreateIndex &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalCopy &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalExplain &op) {
	VisitOperator(op);
}

void LogicalOperatorVisitor::Visit(LogicalWindow &op) {
	VisitOperator(op);
}
