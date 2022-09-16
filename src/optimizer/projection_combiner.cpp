#include "duckdb/optimizer/projection_combiner.hpp"

#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

static void SwapOperators(unique_ptr<LogicalOperator> &op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_PROJECTION);

	// Cut out the projection's child
	auto child = move(op->children[0]);
	op->children[0] = move(child->children[0]);

	// Move the projection under the original child
	child->children[0] = move(op);
	op = move(child);
}

static void TrySwapProjection(unique_ptr<LogicalOperator> &op) {
	auto &limit = (LogicalLimit &)*op->children[0];
	if (limit.limit || limit.offset) {
		// We cannot push it down if there are expressions in the limit
		return;
	}
	SwapOperators(op);
}

static void PushDownProjections(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		for (auto &expr : op->expressions) {
			if (expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				// We only push down projections with bound column references
				return;
			}
		}

		// TODO: add more operators
		switch (op->children[0]->type) {
		case LogicalOperatorType::LOGICAL_LIMIT:
			TrySwapProjection(op);
			break;
		default:
			break;
		}
	}

	for (auto &child : op->children) {
		PushDownProjections(child);
	}
}

static void CombineProjectionOrder(LogicalProjection &projection, unique_ptr<LogicalOperator> &op) {
	auto &order = (LogicalOrder &)*projection.children[0];
	if (order.table_index != DConstants::INVALID_INDEX) {
		// Order is already combined with another projection
		return;
	}
	order.projections = move(projection.expressions);
	for (auto &proj : order.projections) {
		proj->alias.clear();
	}
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
	// TODO: add more operators
	switch (op->children[0]->type) {
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		CombineProjectionOrder(projection, op);
		break;
	default:
		break;
	}
}

unique_ptr<LogicalOperator> ProjectionCombiner::Optimize(unique_ptr<LogicalOperator> op) {
	// Push down projections through non-blocking operators that do not alter the columns in any way
	PushDownProjections(op);
	// Combine projections into operators so we don't need to immediately project them out
	CombineProjections(op);
	return op;
}

} // namespace duckdb
