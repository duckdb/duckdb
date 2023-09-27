#include "duckdb/optimizer/topn_optimizer.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

bool TopN::CanOptimize(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT &&
	    op.children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &limit = op.Cast<LogicalLimit>();

		// When there are some expressions in the limit operator,
		// we shouldn't use this optimizations. Because of the expressions
		// will be lost when it convert to TopN operator.
		if (limit.limit || limit.offset) {
			return false;
		}

		// This optimization doesn't apply when OFFSET is present without LIMIT
		// Or if offset is not constant
		if (limit.limit_val != NumericLimits<int64_t>::Maximum() || limit.offset) {
			return true;
		}
	}
	return false;
}

unique_ptr<LogicalOperator> TopN::Optimize(unique_ptr<LogicalOperator> op) {
	if (CanOptimize(*op)) {
		auto &limit = op->Cast<LogicalLimit>();
		auto &order_by = (op->children[0])->Cast<LogicalOrder>();

		auto topn = make_uniq<LogicalTopN>(std::move(order_by.orders), limit.limit_val, limit.offset_val);
		topn->AddChild(std::move(order_by.children[0]));
		op = std::move(topn);
	} else {
		for (auto &child : op->children) {
			child = Optimize(std::move(child));
		}
	}
	return op;
}

} // namespace duckdb
