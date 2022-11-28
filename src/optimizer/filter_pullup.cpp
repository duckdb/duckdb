#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

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
	case LogicalOperatorType::LOGICAL_EXCEPT:
		return PullupSetOperation(move(op));
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		// we can just pull directly through these operations without any rewriting
		op->children[0] = Rewrite(move(op->children[0]));
		return op;
	}
	default:
		return FinishPullup(move(op));
	}
}

unique_ptr<LogicalOperator> FilterPullup::PullupJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ANY_JOIN || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = (LogicalJoin &)*op;

	switch (join.join_type) {
	case JoinType::INNER:
		return PullupInnerJoin(move(op));
	case JoinType::LEFT:
	case JoinType::ANTI:
	case JoinType::SEMI: {
		return PullupFromLeft(move(op));
	}
	default:
		// unsupported join type: call children pull up
		return FinishPullup(move(op));
	}
}

unique_ptr<LogicalOperator> FilterPullup::PullupInnerJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(((LogicalJoin &)*op).join_type == JoinType::INNER);
	D_ASSERT(op->type != LogicalOperatorType::LOGICAL_DELIM_JOIN);
	return PullupBothSide(move(op));
}

unique_ptr<LogicalOperator> FilterPullup::PullupCrossProduct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
	return PullupBothSide(move(op));
}

unique_ptr<LogicalOperator> FilterPullup::GeneratePullupFilter(unique_ptr<LogicalOperator> child,
                                                               vector<unique_ptr<Expression>> &expressions) {
	unique_ptr<LogicalFilter> filter = make_unique<LogicalFilter>();
	for (idx_t i = 0; i < expressions.size(); ++i) {
		filter->expressions.push_back(move(expressions[i]));
	}
	expressions.clear();
	filter->children.push_back(move(child));
	return move(filter);
}

unique_ptr<LogicalOperator> FilterPullup::FinishPullup(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		FilterPullup pullup;
		op->children[i] = pullup.Rewrite(move(op->children[i]));
	}
	// now pull up any existing filters
	if (filters_expr_pullup.empty()) {
		// no filters to pull up
		return op;
	}
	return GeneratePullupFilter(move(op), filters_expr_pullup);
}

} // namespace duckdb
