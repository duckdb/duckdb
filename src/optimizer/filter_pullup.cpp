#include "duckdb/optimizer/filter_pullup.hpp"

namespace duckdb {
using namespace std;

unique_ptr<LogicalOperator> FilterPullup::Rewrite(unique_ptr<LogicalOperator> op) {
    switch (op->type) {
        case LogicalOperatorType::LOGICAL_FILTER:
            return PullupFilter(move(op));
        case LogicalOperatorType::LOGICAL_PROJECTION:
            return PullupProjection(move(op));
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            return PullupCrossProduct(move(op));
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_ANY_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN:
            return PullupJoin(move(op));
        case LogicalOperatorType::LOGICAL_INTERSECT:
        	return PullupIntersect(move(op));
        default:
		    return FinishPullup(move(op));
    }
}

unique_ptr<LogicalOperator> FilterPullup::PullupJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	       op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = (LogicalJoin &)*op;

	switch (join.join_type) {
    case JoinType::INNER:
   		return PullupInnerJoin(move(op));
	case JoinType::LEFT:
		return PullupLeftJoin(move(op));
	default:
		// unsupported join type: call children pull up
		return FinishPullup(move(op));
	}
}

unique_ptr<LogicalOperator> FilterPullup::PullupInnerJoin(unique_ptr<LogicalOperator> op) {
	auto &join = (LogicalJoin &)*op;
    D_ASSERT(join.join_type == JoinType::INNER);
	D_ASSERT(op->type != LogicalOperatorType::LOGICAL_DELIM_JOIN);
	return PullupBothSide(move(op));
}

unique_ptr<LogicalOperator> FilterPullup::PullupCrossProduct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
	return PullupBothSide(move(op));
}

unique_ptr<LogicalOperator> FilterPullup::FixParenthood(unique_ptr<LogicalOperator> parent, unique_ptr<LogicalOperator> child,
                                                        idx_t parent_child_idx, idx_t child_child_idx) {
    child->children.push_back(move(parent->children[parent_child_idx]));
    parent->children[0] = move(child);
    return parent;
}

unique_ptr<LogicalOperator> FilterPullup::FinishPullup(unique_ptr<LogicalOperator> op) {
    // unhandled type, first perform filter pushdown in its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		FilterPullup pullup(optimizer, root_pullup_node_ptr);
		op->children[i] = pullup.Rewrite(move(op->children[i]));
	}
    return op;
}

} // namespace duckdb