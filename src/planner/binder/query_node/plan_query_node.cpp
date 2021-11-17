#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root) {
	D_ASSERT(root);
	for (auto &mod : node.modifiers) {
		switch (mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &bound = (BoundDistinctModifier &)*mod;
			auto distinct = make_unique<LogicalDistinct>(move(bound.target_distincts));
			distinct->AddChild(move(root));
			root = move(distinct);
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
			auto &bound = (BoundOrderModifier &)*mod;
			auto order = make_unique<LogicalOrder>(move(bound.orders));
			order->AddChild(move(root));
			root = move(order);
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &bound = (BoundLimitModifier &)*mod;
			if (bound.limit_val.IsMaximum()) {
				auto limit = make_unique<LogicalLimit>(bound.limit_val.is_percentage, NumericLimits<int64_t>::Maximum(),
				                                       bound.offset_val, move(bound.limit), move(bound.offset));
				limit->AddChild(move(root));
				root = move(limit);
			} else {
				auto limit =
				    make_unique<LogicalLimit>(bound.limit_val.is_percentage, (int64_t)bound.limit_val.limit_value,
				                              bound.offset_val, move(bound.limit), move(bound.offset));
				limit->AddChild(move(root));
				root = move(limit);
			}
			break;
		}
		default:
			throw BinderException("Unimplemented modifier type!");
		}
	}
	return root;
}

} // namespace duckdb
