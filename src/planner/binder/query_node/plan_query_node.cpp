#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_limit_percent.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root) {
	D_ASSERT(root);
	for (auto &mod : node.modifiers) {
		switch (mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &bound = (BoundDistinctModifier &)*mod;
			auto distinct = make_unique<LogicalDistinct>(std::move(bound.target_distincts));
			distinct->AddChild(std::move(root));
			root = std::move(distinct);
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
			auto &bound = (BoundOrderModifier &)*mod;
			auto order = make_unique<LogicalOrder>(std::move(bound.orders));
			order->AddChild(std::move(root));
			root = std::move(order);
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &bound = (BoundLimitModifier &)*mod;
			auto limit = make_unique<LogicalLimit>(bound.limit_val, bound.offset_val, std::move(bound.limit),
			                                       std::move(bound.offset));
			limit->AddChild(std::move(root));
			root = std::move(limit);
			break;
		}
		case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
			auto &bound = (BoundLimitPercentModifier &)*mod;
			auto limit = make_unique<LogicalLimitPercent>(bound.limit_percent, bound.offset_val, std::move(bound.limit),
			                                              std::move(bound.offset));
			limit->AddChild(std::move(root));
			root = std::move(limit);
			break;
		}
		default:
			throw BinderException("Unimplemented modifier type!");
		}
	}
	return root;
}

} // namespace duckdb
