#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/expression_util.hpp"

namespace duckdb {

bool ResultModifier::Equals(const ResultModifier *other) const {
	if (!other) {
		return false;
	}
	return type == other->type;
}

void ResultModifier::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<ResultModifierType>(type);
	Serialize(writer);
	writer.Finalize();
}

unique_ptr<ResultModifier> ResultModifier::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto type = reader.ReadRequired<ResultModifierType>();

	unique_ptr<ResultModifier> result;
	switch (type) {
	case ResultModifierType::LIMIT_MODIFIER:
		result = LimitModifier::Deserialize(reader);
		break;
	case ResultModifierType::ORDER_MODIFIER:
		result = OrderModifier::Deserialize(reader);
		break;
	case ResultModifierType::DISTINCT_MODIFIER:
		result = DistinctModifier::Deserialize(reader);
		break;
	case ResultModifierType::LIMIT_PERCENT_MODIFIER:
		result = LimitPercentModifier::Deserialize(reader);
		break;
	default:
		throw InternalException("Unrecognized ResultModifierType for Deserialization");
	}
	reader.Finalize();
	return result;
}

bool LimitModifier::Equals(const ResultModifier *other_p) const {
	if (!ResultModifier::Equals(other_p)) {
		return false;
	}
	auto &other = (LimitModifier &)*other_p;
	if (!BaseExpression::Equals(limit.get(), other.limit.get())) {
		return false;
	}
	if (!BaseExpression::Equals(offset.get(), other.offset.get())) {
		return false;
	}
	return true;
}

unique_ptr<ResultModifier> LimitModifier::Copy() const {
	auto copy = make_unique<LimitModifier>();
	if (limit) {
		copy->limit = limit->Copy();
	}
	if (offset) {
		copy->offset = offset->Copy();
	}
	return move(copy);
}

void LimitModifier::Serialize(FieldWriter &writer) const {
	writer.WriteOptional(limit);
	writer.WriteOptional(offset);
}

unique_ptr<ResultModifier> LimitModifier::Deserialize(FieldReader &reader) {
	auto mod = make_unique<LimitModifier>();
	mod->limit = reader.ReadOptional<ParsedExpression>(nullptr);
	mod->offset = reader.ReadOptional<ParsedExpression>(nullptr);
	return move(mod);
}

bool DistinctModifier::Equals(const ResultModifier *other_p) const {
	if (!ResultModifier::Equals(other_p)) {
		return false;
	}
	auto &other = (DistinctModifier &)*other_p;
	if (!ExpressionUtil::ListEquals(distinct_on_targets, other.distinct_on_targets)) {
		return false;
	}
	return true;
}

unique_ptr<ResultModifier> DistinctModifier::Copy() const {
	auto copy = make_unique<DistinctModifier>();
	for (auto &expr : distinct_on_targets) {
		copy->distinct_on_targets.push_back(expr->Copy());
	}
	return move(copy);
}

void DistinctModifier::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(distinct_on_targets);
}

unique_ptr<ResultModifier> DistinctModifier::Deserialize(FieldReader &reader) {
	auto mod = make_unique<DistinctModifier>();
	mod->distinct_on_targets = reader.ReadRequiredSerializableList<ParsedExpression>();
	return move(mod);
}

bool OrderModifier::Equals(const ResultModifier *other_p) const {
	if (!ResultModifier::Equals(other_p)) {
		return false;
	}
	auto &other = (OrderModifier &)*other_p;
	if (orders.size() != other.orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < orders.size(); i++) {
		if (orders[i].type != other.orders[i].type) {
			return false;
		}
		if (!BaseExpression::Equals(orders[i].expression.get(), other.orders[i].expression.get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<ResultModifier> OrderModifier::Copy() const {
	auto copy = make_unique<OrderModifier>();
	for (auto &order : orders) {
		copy->orders.emplace_back(order.type, order.null_order, order.expression->Copy());
	}
	return move(copy);
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

void OrderByNode::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<OrderType>(type);
	writer.WriteField<OrderByNullType>(null_order);
	writer.WriteSerializable(*expression);
	writer.Finalize();
}

OrderByNode OrderByNode::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto type = reader.ReadRequired<OrderType>();
	auto null_order = reader.ReadRequired<OrderByNullType>();
	auto expression = reader.ReadRequiredSerializable<ParsedExpression>();
	reader.Finalize();
	return OrderByNode(type, null_order, move(expression));
}

void OrderModifier::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(orders);
}

unique_ptr<ResultModifier> OrderModifier::Deserialize(FieldReader &reader) {
	auto mod = make_unique<OrderModifier>();
	mod->orders = reader.ReadRequiredSerializableList<OrderByNode, OrderByNode>();
	return move(mod);
}

bool LimitPercentModifier::Equals(const ResultModifier *other_p) const {
	if (!ResultModifier::Equals(other_p)) {
		return false;
	}
	auto &other = (LimitPercentModifier &)*other_p;
	if (!BaseExpression::Equals(limit.get(), other.limit.get())) {
		return false;
	}
	if (!BaseExpression::Equals(offset.get(), other.offset.get())) {
		return false;
	}
	return true;
}

unique_ptr<ResultModifier> LimitPercentModifier::Copy() const {
	auto copy = make_unique<LimitPercentModifier>();
	if (limit) {
		copy->limit = limit->Copy();
	}
	if (offset) {
		copy->offset = offset->Copy();
	}
	return move(copy);
}

void LimitPercentModifier::Serialize(FieldWriter &writer) const {
	writer.WriteOptional(limit);
	writer.WriteOptional(offset);
}

unique_ptr<ResultModifier> LimitPercentModifier::Deserialize(FieldReader &reader) {
	auto mod = make_unique<LimitPercentModifier>();
	mod->limit = reader.ReadOptional<ParsedExpression>(nullptr);
	mod->offset = reader.ReadOptional<ParsedExpression>(nullptr);
	return move(mod);
}

} // namespace duckdb
