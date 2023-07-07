#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/expression_util.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

bool ResultModifier::Equals(const ResultModifier &other) const {
	return type == other.type;
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

bool LimitModifier::Equals(const ResultModifier &other_p) const {
	if (!ResultModifier::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<LimitModifier>();
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
	if (limit) {
		copy->limit = limit->Copy();
	}
	if (offset) {
		copy->offset = offset->Copy();
	}
	return std::move(copy);
}

void LimitModifier::Serialize(FieldWriter &writer) const {
	writer.WriteOptional(limit);
	writer.WriteOptional(offset);
}

unique_ptr<ResultModifier> LimitModifier::Deserialize(FieldReader &reader) {
	auto mod = make_uniq<LimitModifier>();
	mod->limit = reader.ReadOptional<ParsedExpression>(nullptr);
	mod->offset = reader.ReadOptional<ParsedExpression>(nullptr);
	return std::move(mod);
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

void DistinctModifier::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(distinct_on_targets);
}

unique_ptr<ResultModifier> DistinctModifier::Deserialize(FieldReader &reader) {
	auto mod = make_uniq<DistinctModifier>();
	mod->distinct_on_targets = reader.ReadRequiredSerializableList<ParsedExpression>();
	return std::move(mod);
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

void OrderByNode::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<OrderType>(type);
	writer.WriteField<OrderByNullType>(null_order);
	writer.WriteSerializable(*expression);
	writer.Finalize();
}

void OrderByNode::FormatSerialize(FormatSerializer &serializer) const {
	serializer.WriteProperty("type", type);
	serializer.WriteProperty("null_order", null_order);
	serializer.WriteProperty("expression", expression);
}

OrderByNode OrderByNode::FormatDeserialize(FormatDeserializer &deserializer) {
	auto type = deserializer.ReadProperty<OrderType>("type");
	auto null_order = deserializer.ReadProperty<OrderByNullType>("null_order");
	auto expression = deserializer.ReadProperty<unique_ptr<ParsedExpression>>("expression");
	return OrderByNode(type, null_order, std::move(expression));
}

OrderByNode OrderByNode::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto type = reader.ReadRequired<OrderType>();
	auto null_order = reader.ReadRequired<OrderByNullType>();
	auto expression = reader.ReadRequiredSerializable<ParsedExpression>();
	reader.Finalize();
	return OrderByNode(type, null_order, std::move(expression));
}

void OrderModifier::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(orders);
}

unique_ptr<ResultModifier> OrderModifier::Deserialize(FieldReader &reader) {
	auto mod = make_uniq<OrderModifier>();
	mod->orders = reader.ReadRequiredSerializableList<OrderByNode, OrderByNode>();
	return std::move(mod);
}

bool LimitPercentModifier::Equals(const ResultModifier &other_p) const {
	if (!ResultModifier::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<LimitPercentModifier>();
	if (!ParsedExpression::Equals(limit, other.limit)) {
		return false;
	}
	if (!ParsedExpression::Equals(offset, other.offset)) {
		return false;
	}
	return true;
}

unique_ptr<ResultModifier> LimitPercentModifier::Copy() const {
	auto copy = make_uniq<LimitPercentModifier>();
	if (limit) {
		copy->limit = limit->Copy();
	}
	if (offset) {
		copy->offset = offset->Copy();
	}
	return std::move(copy);
}

void LimitPercentModifier::Serialize(FieldWriter &writer) const {
	writer.WriteOptional(limit);
	writer.WriteOptional(offset);
}

unique_ptr<ResultModifier> LimitPercentModifier::Deserialize(FieldReader &reader) {
	auto mod = make_uniq<LimitPercentModifier>();
	mod->limit = reader.ReadOptional<ParsedExpression>(nullptr);
	mod->offset = reader.ReadOptional<ParsedExpression>(nullptr);
	return std::move(mod);
}

} // namespace duckdb
