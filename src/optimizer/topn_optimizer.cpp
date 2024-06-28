#include "duckdb/optimizer/topn_optimizer.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

bool TopN::CanOptimize(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op.Cast<LogicalLimit>();

		if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
			// we need LIMIT to be present AND be a constant value for us to be able to use Top-N
			return false;
		}
		if (limit.offset_val.Type() == LimitNodeType::EXPRESSION_VALUE) {
			// we need offset to be either not set (i.e. limit without offset) OR have offset be
			return false;
		}

		auto child_op = op.children[0].get();

		while (child_op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			D_ASSERT(!child_op->children.empty());
			child_op = child_op->children[0].get();
		}

		return child_op->type == LogicalOperatorType::LOGICAL_ORDER_BY;
	}
	return false;
}

unique_ptr<LogicalOperator> TopN::Optimize(unique_ptr<LogicalOperator> op) {
	if (CanOptimize(*op)) {

		vector<unique_ptr<LogicalOperator>> projections;

		// traverse operator tree and collect all projection nodes until we reach
		// the order by operator

		auto child = std::move(op->children[0]);
		// collect all projections until we get to the order by
		while (child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			D_ASSERT(!child->children.empty());
			auto tmp = std::move(child->children[0]);
			projections.push_back(std::move(child));
			child = std::move(tmp);
		}
		D_ASSERT(child->type == LogicalOperatorType::LOGICAL_ORDER_BY);
		auto &order_by = child->Cast<LogicalOrder>();

		// Move order by operator into children of limit operator
		op->children[0] = std::move(child);

		auto &limit = op->Cast<LogicalLimit>();
		auto limit_val = limit.limit_val.GetConstantValue();
		idx_t offset_val = 0;
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			offset_val = limit.offset_val.GetConstantValue();
		}
		auto topn = make_uniq<LogicalTopN>(std::move(order_by.orders), limit_val, offset_val);
		topn->AddChild(std::move(order_by.children[0]));
		op = std::move(topn);

		// reconstruct all projection nodes above limit operator
		while (!projections.empty()) {
			auto node = std::move(projections.back());
			node->children[0] = std::move(op);
			op = std::move(node);
			projections.pop_back();
		}
	}

	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	return op;
}

} // namespace duckdb
