#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/parser/expression_map.hpp"

namespace duckdb {

BoundResultModifier::BoundResultModifier(ResultModifierType type) : type(type) {
}

BoundResultModifier::~BoundResultModifier() {
}

BoundOrderByNode::BoundOrderByNode(OrderType type, OrderByNullType null_order, unique_ptr<Expression> expression)
    : type(type), null_order(null_order), expression(std::move(expression)) {
}
BoundOrderByNode::BoundOrderByNode(OrderType type, OrderByNullType null_order, unique_ptr<Expression> expression,
                                   unique_ptr<BaseStatistics> stats)
    : type(type), null_order(null_order), expression(std::move(expression)), stats(std::move(stats)) {
}

BoundOrderByNode BoundOrderByNode::Copy() const {
	if (stats) {
		return BoundOrderByNode(type, null_order, expression->Copy(), stats->ToUnique());
	} else {
		return BoundOrderByNode(type, null_order, expression->Copy());
	}
}

bool BoundOrderByNode::Equals(const BoundOrderByNode &other) const {
	if (type != other.type || null_order != other.null_order) {
		return false;
	}
	if (!expression->Equals(*other.expression)) {
		return false;
	}

	return true;
}

string BoundOrderByNode::ToString() const {
	auto str = expression->ToString();
	switch (type) {
	case OrderType::ASCENDING:
		str += " ASC";
		break;
	case OrderType::DESCENDING:
		str += " DESC";
		break;
	default:
		break;
	}

	switch (null_order) {
	case OrderByNullType::NULLS_FIRST:
		str += " NULLS FIRST";
		break;
	case OrderByNullType::NULLS_LAST:
		str += " NULLS LAST";
		break;
	default:
		break;
	}
	return str;
}

unique_ptr<BoundOrderModifier> BoundOrderModifier::Copy() const {
	auto result = make_uniq<BoundOrderModifier>();
	for (auto &order : orders) {
		result->orders.push_back(order.Copy());
	}
	return result;
}

bool BoundOrderModifier::Equals(const BoundOrderModifier &left, const BoundOrderModifier &right) {
	if (left.orders.size() != right.orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.orders.size(); i++) {
		if (!left.orders[i].Equals(right.orders[i])) {
			return false;
		}
	}
	return true;
}

bool BoundOrderModifier::Equals(const unique_ptr<BoundOrderModifier> &left,
                                const unique_ptr<BoundOrderModifier> &right) {
	if (left.get() == right.get()) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return BoundOrderModifier::Equals(*left, *right);
}

bool BoundOrderModifier::Simplify(const vector<unique_ptr<Expression>> &groups) {
	// for each ORDER BY - check if it is actually necessary
	// expressions that are in the groups do not need to be ORDERED BY
	// `ORDER BY` on a group has no effect, because for each aggregate, the group is unique
	// similarly, we only need to ORDER BY each aggregate once
	expression_set_t seen_expressions;
	for (auto &target : groups) {
		seen_expressions.insert(*target);
	}
	vector<BoundOrderByNode> new_order_nodes;
	for (auto &order_node : orders) {
		if (seen_expressions.find(*order_node.expression) != seen_expressions.end()) {
			// we do not need to order by this node
			continue;
		}
		seen_expressions.insert(*order_node.expression);
		new_order_nodes.push_back(std::move(order_node));
	}
	orders.swap(new_order_nodes);

	return orders.empty();
}

BoundLimitModifier::BoundLimitModifier() : BoundResultModifier(ResultModifierType::LIMIT_MODIFIER) {
}

BoundOrderModifier::BoundOrderModifier() : BoundResultModifier(ResultModifierType::ORDER_MODIFIER) {
}

BoundDistinctModifier::BoundDistinctModifier() : BoundResultModifier(ResultModifierType::DISTINCT_MODIFIER) {
}

BoundLimitPercentModifier::BoundLimitPercentModifier()
    : BoundResultModifier(ResultModifierType::LIMIT_PERCENT_MODIFIER) {
}

} // namespace duckdb
