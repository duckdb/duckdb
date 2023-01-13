#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/common/field_writer.hpp"

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
		return BoundOrderByNode(type, null_order, expression->Copy(), stats->Copy());
	} else {
		return BoundOrderByNode(type, null_order, expression->Copy());
	}
}

bool BoundOrderByNode::Equals(const BoundOrderByNode &other) const {
	if (type != other.type || null_order != other.null_order) {
		return false;
	}
	if (!expression->Equals(other.expression.get())) {
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

void BoundOrderByNode::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField(type);
	writer.WriteField(null_order);
	writer.WriteSerializable(*expression);
	// TODO statistics
	writer.Finalize();
}

BoundOrderByNode BoundOrderByNode::Deserialize(Deserializer &source, PlanDeserializationState &state) {
	FieldReader reader(source);
	auto type = reader.ReadRequired<OrderType>();
	auto null_order = reader.ReadRequired<OrderByNullType>();
	auto expression = reader.ReadRequiredSerializable<Expression>(state);
	reader.Finalize();
	return BoundOrderByNode(type, null_order, std::move(expression));
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
