#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/common/extra_type_info.hpp"

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

LogicalType BoundOrderModifier::GetSortKeyType() const {
	vector<OrderBySpec> order_specs;
	for (const auto &order_by : orders) {
		const auto has_null = order_by.stats ? order_by.stats->CanHaveNull() : true;
		order_specs.emplace_back(order_by.type, order_by.null_order, order_by.expression->return_type, has_null);
	}

	return LogicalType::SORT_KEY(order_specs);
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
