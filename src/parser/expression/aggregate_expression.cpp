
#include "parser/expression/aggregate_expression.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

AggregateExpression::AggregateExpression(ExpressionType type, bool distinct,
                                         unique_ptr<Expression> child)
    : Expression(type), index(0) {
	this->distinct = distinct;

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
	case ExpressionType::AGGREGATE_SUM:
	case ExpressionType::AGGREGATE_MIN:
	case ExpressionType::AGGREGATE_MAX:
	case ExpressionType::AGGREGATE_FIRST:

		break;
	default:
		throw NotImplementedException("Aggregate type not supported");
	}
	if (child) {
		AddChild(move(child));
	}
}

//! Resolve the type of the aggregate
void AggregateExpression::ResolveType() {
	Expression::ResolveType();
	switch (type) {
	case ExpressionType::AGGREGATE_COUNT_STAR:
		return_type = TypeId::BIGINT;
		break;
	case ExpressionType::AGGREGATE_COUNT:
		if (children[0]->IsScalar()) {
			stats.has_stats = false;
		} else {
			Statistics::Count(children[0]->stats, stats);
		}
		return_type = TypeId::BIGINT;
		break;
	case ExpressionType::AGGREGATE_MAX:
		Statistics::Max(children[0]->stats, stats);
		return_type = max(children[0]->return_type, stats.MinimalType());
		break;
	case ExpressionType::AGGREGATE_MIN:
		Statistics::Min(children[0]->stats, stats);
		return_type = max(children[0]->return_type, stats.MinimalType());
		break;
	case ExpressionType::AGGREGATE_SUM:
		if (children[0]->IsScalar()) {
			stats.has_stats = false;
			switch (children[0]->return_type) {
			case TypeId::BOOLEAN:
			case TypeId::TINYINT:
			case TypeId::SMALLINT:
			case TypeId::INTEGER:
			case TypeId::BIGINT:
				return_type = TypeId::BIGINT;
				break;
			default:
				return_type = children[0]->return_type;
			}
		} else {
			Statistics::Count(children[0]->stats, stats);
			Statistics::Sum(children[0]->stats, stats);
			return_type = max(children[0]->return_type, stats.MinimalType());
		}

		break;
	case ExpressionType::AGGREGATE_FIRST:
		return_type = children[0]->return_type;
		break;
	default:
		throw NotImplementedException("Unsupported aggregate type!");
	}
}

void AggregateExpression::GetAggregates(
    vector<AggregateExpression *> &expressions) {
	size_t size = expressions.size();
	Expression::GetAggregates(expressions);
	if (size == expressions.size()) {
		// we only want the lowest level aggregates
		expressions.push_back(this);
	}
}

unique_ptr<Expression> AggregateExpression::Copy() {
	if (children.size() > 1) {
		assert(0);
		return nullptr;
	}
	auto child = children.size() == 1 ? children[0]->Copy() : nullptr;
	auto new_aggregate =
	    make_unique<AggregateExpression>(type, distinct, move(child));
	new_aggregate->index = index;
	new_aggregate->CopyProperties(*this);
	return new_aggregate;
}

void AggregateExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.Write<bool>(distinct);
}

unique_ptr<Expression>
AggregateExpression::Deserialize(ExpressionDeserializeInformation *info,
                                 Deserializer &source) {
	auto distinct = source.Read<bool>();

	if (info->children.size() > 1) {
		throw SerializationException(
		    "More than one child for aggregate expression!");
	}

	auto child = info->children.size() == 0 ? nullptr : move(info->children[0]);
	return make_unique<AggregateExpression>(info->type, distinct, move(child));
}

string AggregateExpression::GetName() {
	if (!alias.empty()) {
		return alias;
	}
	switch (type) {
	case ExpressionType::AGGREGATE_COUNT:
	case ExpressionType::AGGREGATE_COUNT_STAR:
		return "COUNT";
	case ExpressionType::AGGREGATE_SUM:
		return "SUM";
	case ExpressionType::AGGREGATE_MIN:
		return "MIN";
	case ExpressionType::AGGREGATE_MAX:
		return "MAX";
	case ExpressionType::AGGREGATE_FIRST:
		return "FIRST";
	default:
		return "UNKNOWN";
	}
}
