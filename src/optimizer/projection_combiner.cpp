#include "duckdb/optimizer/projection_combiner.hpp"

namespace duckdb {

static void CombineProjectionOrder(LogicalProjection &projection, unique_ptr<LogicalOperator> &op) {
	auto &order = (LogicalOrder &)*projection.children[0];
	order.projections = move(projection.expressions);
	order.table_index = projection.table_index;
	op = move(op->children[0]);
}

static void CombineProjections(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		CombineProjections(child);
	}

	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}

	for (auto &expr : op->expressions) {
		if (expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			// We only combine bound column references into other operators
			return;
		}
	}

	auto &projection = (LogicalProjection &)*op;
	switch (op->children[0]->type) {
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		CombineProjectionOrder(projection, op);
		break;
	default:
		break;
	}
}

unique_ptr<LogicalOperator> ProjectionCombiner::Optimize(unique_ptr<LogicalOperator> op) {
	CombineProjections(op);
	return move(op);
}

} // namespace duckdb
