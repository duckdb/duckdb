#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> TopN::Optimize(unique_ptr<LogicalOperator> op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT &&
	    op->children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &limit = (LogicalLimit &)*op;
		auto &order_by = (LogicalOrder &)*(op->children[0]);

		// This optimization doesn't apply when OFFSET is present without LIMIT
		// Or if offset is not constant
		LimitCount limit_count;
		limit_count.SetLimitValue(limit.is_limit_percent, limit.limit_val);
		if (limit.limit_val != (double)NumericLimits<int64_t>::Maximum() || limit.offset) {
			auto topn = make_unique<LogicalTopN>(move(order_by.orders), limit_count, limit.offset_val);
			topn->AddChild(move(order_by.children[0]));
			op = move(topn);
		}
	} else {
		for (auto &child : op->children) {
			child = Optimize(move(child));
		}
	}
	return op;
}

} // namespace duckdb
