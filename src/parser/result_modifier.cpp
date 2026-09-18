#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/expression_util.hpp"

namespace duckdb {

bool ResultModifier::Equals(const ResultModifier &other) const {
	return type == other.type;
}

bool LimitModifier::Equals(const ResultModifier &other_p) const {
	if (!ResultModifier::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<LimitModifier>();
	if (limit_type != other.limit_type) {
		return false;
	}
	if (!ParsedExpression::Equals(limit, other.limit)) {
		return false;
	}
	if (!ParsedExpression::Equals(offset, other.offset)) {
		return false;
	}
	return true;
}

unique_ptr<ResultModifier> LimitModifier::Copy() const {
	auto copy = make_uniq<LimitModifier>();
	copy->limit_type = limit_type;
	if (limit) {
		copy->limit = limit->Copy();
	}
	if (offset) {
		copy->offset = offset->Copy();
	}
	return std::move(copy);
}

bool LimitModifier::UseLegacySerialization() const {
	return limit_type == LimitValueType::PERCENTAGE;
}

void LimitModifier::LegacySerialize(Serializer &serializer) const {
	LegacyLimitPercentModifier legacy_serialization;
	if (limit) {
		legacy_serialization.limit = limit->Copy();
	}
	if (offset) {
		legacy_serialization.offset = offset->Copy();
	}
	legacy_serialization.Serialize(serializer);
}

bool DistinctModifier::Equals(const ResultModifier &other_p) const {
	if (!ResultModifier::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<DistinctModifier>();
	if (!ExpressionUtil::ListEquals(distinct_on_targets, other.distinct_on_targets)) {
		return false;
	}
	return true;
}

unique_ptr<ResultModifier> DistinctModifier::Copy() const {
	auto copy = make_uniq<DistinctModifier>();
	for (auto &expr : distinct_on_targets) {
		copy->distinct_on_targets.push_back(expr->Copy());
	}
	return std::move(copy);
}

bool OrderModifier::Equals(const ResultModifier &other_p) const {
	if (!ResultModifier::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<OrderModifier>();
	if (orders.size() != other.orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < orders.size(); i++) {
		if (orders[i].type != other.orders[i].type) {
			return false;
		}
		if (!BaseExpression::Equals(*orders[i].expression, *other.orders[i].expression)) {
			return false;
		}
	}
	return true;
}

bool OrderModifier::Equals(const unique_ptr<OrderModifier> &left, const unique_ptr<OrderModifier> &right) {
	if (left.get() == right.get()) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return left->Equals(*right);
}

unique_ptr<ResultModifier> OrderModifier::Copy() const {
	auto copy = make_uniq<OrderModifier>();
	for (auto &order : orders) {
		copy->orders.emplace_back(order.type, order.null_order, order.expression->Copy());
	}
	return std::move(copy);
}

string OrderByNode::ToString() const {
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

bool LegacyLimitPercentModifier::Equals(const ResultModifier &other) const {
	throw InternalException("Legacy Limit Percent modifier should not be used anymore");
}

unique_ptr<ResultModifier> LegacyLimitPercentModifier::Copy() const {
	throw InternalException("Legacy Limit Percent modifier should not be used anymore");
}

unique_ptr<ResultModifier>
LegacyLimitPercentModifier::DeserializeLegacyLimitPercentModifier(unique_ptr<ParsedExpression> limit_p,
                                                                  unique_ptr<ParsedExpression> offset_p) {
	auto result = make_uniq<LimitModifier>();
	result->limit = std::move(limit_p);
	result->offset = std::move(offset_p);
	result->limit_type = LimitValueType::PERCENTAGE;
	return std::move(result);
}

} // namespace duckdb
