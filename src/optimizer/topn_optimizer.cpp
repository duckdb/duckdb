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

		if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
			// we need LIMIT to be present AND be a constant value for us to be able to use Top-N
			return false;
		}
		if (limit.offset_val.Type() == LimitNodeType::EXPRESSION_VALUE) {
			// we need offset to be either not set (i.e. limit without offset) OR have offset be
			return false;
		}
		return true;
	}
	return false;
}

unique_ptr<LogicalOperator> TopN::Optimize(unique_ptr<LogicalOperator> op) {
	if (CanOptimize(*op)) {
		auto &limit = op->Cast<LogicalLimit>();
		auto &order_by = (op->children[0])->Cast<LogicalOrder>();
		auto limit_val = int64_t(limit.limit_val.GetConstantValue());
		int64_t offset_val = 0;
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			offset_val = limit.offset_val.GetConstantValue();
		}
		auto topn = make_uniq<LogicalTopN>(std::move(order_by.orders), limit_val, offset_val);
		topn->AddChild(std::move(order_by.children[0]));
		op = std::move(topn);
	} else {
		// Check if we can push the limit operator down through projection operators
		if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
			auto &limit = op->Cast<LogicalLimit>();
			if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE &&
			    limit.offset_val.Type() != LimitNodeType::EXPRESSION_VALUE) {
				auto child_op = op->children[0].get();
				bool can_optimize = false;

				// Check if there are only projection operators between the limit operator
				// and an order by operator.
				while (child_op && !can_optimize) {
					switch (child_op->type) {
					case LogicalOperatorType::LOGICAL_PROJECTION:
						child_op = child_op->children[0].get();
						if (child_op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
							can_optimize = true;
						}
						break;
					default:
						child_op = nullptr;
						can_optimize = false;
						break;
					}
				}

				if (can_optimize) {
					// traverse operator tree and collect all projection nodes until we reach
					// the order by operator
					vector<unique_ptr<LogicalOperator>> projections;
					projections.push_back(std::move(op->children[0]));
					while (true) {
						auto child = projections.back().get();
						if (child->type != LogicalOperatorType::LOGICAL_PROJECTION) {
							break;
						}
						projections.push_back(std::move(child->children[0]));
					}

					// Move order by operator into children of limit operator
					auto order_by = std::move(projections.back());
					projections.pop_back();
					op->children.clear();
					op->children.push_back(std::move(order_by));

					// reconstruct all projection nodes above limit operator
					unique_ptr<LogicalOperator> root = std::move(op);
					while (!projections.empty()) {
						unique_ptr<LogicalOperator> node = std::move(projections.back());
						node->children.clear();
						node->children.push_back(std::move(root));
						root = std::move(node);
						projections.pop_back();
					}

					// recurse into Optimize function to apply TopN optimization
					return Optimize(std::move(root));
				}
			}
		}

		for (auto &child : op->children) {
			child = Optimize(std::move(child));
		}
	}
	return op;
}

} // namespace duckdb
