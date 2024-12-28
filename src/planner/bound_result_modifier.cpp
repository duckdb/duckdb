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

bool BoundOrderModifier::Simplify(vector<BoundOrderByNode> &orders, const vector<unique_ptr<Expression>> &groups) {
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

	return orders.empty(); // NOLINT
}

bool BoundOrderModifier::Simplify(const vector<unique_ptr<Expression>> &groups) {
	return Simplify(orders, groups);
}

BoundLimitNode::BoundLimitNode(LimitNodeType type, idx_t constant_integer, double constant_percentage,
                               unique_ptr<Expression> expression_p)
    : type(type), constant_integer(constant_integer), constant_percentage(constant_percentage),
      expression(std::move(expression_p)) {
}

BoundLimitNode::BoundLimitNode() : type(LimitNodeType::UNSET) {
}

BoundLimitNode::BoundLimitNode(int64_t constant_value)
    : type(LimitNodeType::CONSTANT_VALUE), constant_integer(NumericCast<idx_t>(constant_value)) {
}

BoundLimitNode::BoundLimitNode(double percentage_value)
    : type(LimitNodeType::CONSTANT_PERCENTAGE), constant_percentage(percentage_value) {
}

BoundLimitNode::BoundLimitNode(unique_ptr<Expression> expression_p, bool is_percentage)
    : type(is_percentage ? LimitNodeType::EXPRESSION_PERCENTAGE : LimitNodeType::EXPRESSION_VALUE),
      expression(std::move(expression_p)) {
}

BoundLimitNode BoundLimitNode::ConstantValue(int64_t value) {
	return BoundLimitNode(value);
}

BoundLimitNode BoundLimitNode::ConstantPercentage(double percentage) {
	return BoundLimitNode(percentage);
}

BoundLimitNode BoundLimitNode::ExpressionValue(unique_ptr<Expression> expression) {
	return BoundLimitNode(std::move(expression), false);
}

BoundLimitNode BoundLimitNode::ExpressionPercentage(unique_ptr<Expression> expression) {
	return BoundLimitNode(std::move(expression), true);
}

idx_t BoundLimitNode::GetConstantValue() const {
	if (Type() != LimitNodeType::CONSTANT_VALUE) {
		throw InternalException("BoundLimitNode::GetConstantValue called but limit is not a constant value");
	}
	return constant_integer;
}

double BoundLimitNode::GetConstantPercentage() const {
	if (Type() != LimitNodeType::CONSTANT_PERCENTAGE) {
		throw InternalException("BoundLimitNode::GetConstantPercentage called but limit is not a constant percentage");
	}
	return constant_percentage;
}

const Expression &BoundLimitNode::GetValueExpression() const {
	if (Type() != LimitNodeType::EXPRESSION_VALUE) {
		throw InternalException("BoundLimitNode::GetValueExpression called but limit is not an expression value");
	}
	return *expression;
}

const Expression &BoundLimitNode::GetPercentageExpression() const {
	if (Type() != LimitNodeType::EXPRESSION_PERCENTAGE) {
		throw InternalException(
		    "BoundLimitNode::GetPercentageExpression called but limit is not an expression percentage");
	}
	return *expression;
}

BoundLimitModifier::BoundLimitModifier() : BoundResultModifier(ResultModifierType::LIMIT_MODIFIER) {
}

BoundOrderModifier::BoundOrderModifier() : BoundResultModifier(ResultModifierType::ORDER_MODIFIER) {
}

BoundDistinctModifier::BoundDistinctModifier() : BoundResultModifier(ResultModifierType::DISTINCT_MODIFIER) {
}

} // namespace duckdb
