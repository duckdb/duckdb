#include "duckdb/optimizer/projection_combiner.hpp"

namespace duckdb {

static void CombineProjections(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		CombineProjections(child);
	}

	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &projection = (LogicalProjection &)*op;

	switch (op->children[0]->type) {
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = (LogicalOrder &)*projection.children[0];
		order.projections = move(projection.expressions);
		order.table_index = projection.table_index;
		op = move(op->children[0]);
		break;
	}
	default:
		break;
	}
}

unique_ptr<LogicalOperator> ProjectionCombiner::Optimize(unique_ptr<LogicalOperator> op) {
	CombineProjections(op);
	return move(op);
}

} // namespace duckdb