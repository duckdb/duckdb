#include "duckdb/optimizer/join_filter_pushdown.hpp"

namespace duckdb {

void JoinFilterPushdownOptimizer::VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {

	}
	LogicalOperatorVisitor::VisitOperator(op);
}

} // namespace duckdb
