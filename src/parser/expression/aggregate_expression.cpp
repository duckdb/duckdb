#include "parser/expression/aggregate_expression.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

AggregateExpression::AggregateExpression(ExpressionType type, unique_ptr<ParsedExpression> child)
    : ParsedExpression(type, ExpressionClass::AGGREGATE) {

	// translate COUNT(*) into AGGREGATE_COUNT_STAR
	if (type == ExpressionType::AGGREGATE_COUNT) {
		if (!child) {
			this->type = ExpressionType::AGGREGATE_COUNT_STAR;
		} else if (child->GetExpressionType() == ExpressionType::STAR) {
			child = nullptr;
			this->type = ExpressionType::AGGREGATE_COUNT_STAR;
		}
	}
	switch (type) {
	case ExpressionType::AGGREGATE_COUNT:
	case ExpressionType::AGGREGATE_COUNT_STAR:
	case ExpressionType::AGGREGATE_COUNT_DISTINCT:
	case ExpressionType::AGGREGATE_SUM:
	case ExpressionType::AGGREGATE_SUM_DISTINCT:
	case ExpressionType::AGGREGATE_MIN:
	case ExpressionType::AGGREGATE_MAX:
	case ExpressionType::AGGREGATE_FIRST:
	case ExpressionType::AGGREGATE_STDDEV_SAMP:
		break;
	default:
		throw NotImplementedException("Aggregate type not supported");
	}
	this->child = move(child);
}

string AggregateExpression::ToString() const {
	return ExpressionTypeToString(type) + "(" + (child ? child->ToString() : string()) + ")";
}

bool AggregateExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (AggregateExpression *)other_;
	if (child) {
		if (!child->Equals(other->child.get())) {
			return false;
		}
	} else if (other->child) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> AggregateExpression::Copy() const {
	auto new_child = child ? child->Copy() : nullptr;
	auto new_aggregate = make_unique<AggregateExpression>(type, move(new_child));
	new_aggregate->CopyProperties(*this);
	return move(new_aggregate);
}

void AggregateExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteOptional(child);
}

unique_ptr<ParsedExpression> AggregateExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto child = source.ReadOptional<ParsedExpression>();
	return make_unique<AggregateExpression>(type, move(child));
}
