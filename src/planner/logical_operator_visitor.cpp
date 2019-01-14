#include "planner/logical_operator_visitor.hpp"

#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalOperatorVisitor::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

void LogicalOperatorVisitor::VisitOperatorChildren(LogicalOperator &op) {
	for (auto &child : op.children) {
		VisitOperator(*child);
	}
}

void LogicalOperatorVisitor::VisitOperatorExpressions(LogicalOperator &op) {
	for (size_t i = 0, child_count = op.ExpressionCount(); i < child_count; i++) {
		op.ReplaceExpression(
		    [&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
			    VisitExpression(&child);
			    return child;
		    },
		    i);
	}
}
