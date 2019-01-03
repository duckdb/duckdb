#include "parser/expression/aggregate_expression.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

AggregateExpression::AggregateExpression(ExpressionType type, unique_ptr<Expression> child)
    : Expression(type), index(0) {

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

//! Resolve the type of the aggregate
void AggregateExpression::ResolveType() {
	Expression::ResolveType();
	switch (type) {
	case ExpressionType::AGGREGATE_COUNT_STAR:
		return_type = TypeId::BIGINT;
		break;
	case ExpressionType::AGGREGATE_COUNT:
	case ExpressionType::AGGREGATE_COUNT_DISTINCT:
		if (child->IsScalar()) {
			stats.has_stats = false;
		} else {
			ExpressionStatistics::Count(child->stats, stats);
		}
		return_type = TypeId::BIGINT;
		break;
	case ExpressionType::AGGREGATE_MAX:
		ExpressionStatistics::Max(child->stats, stats);
		return_type = max(child->return_type, stats.MinimalType());
		break;
	case ExpressionType::AGGREGATE_MIN:
		ExpressionStatistics::Min(child->stats, stats);
		return_type = max(child->return_type, stats.MinimalType());
		break;
	case ExpressionType::AGGREGATE_SUM:
	case ExpressionType::AGGREGATE_SUM_DISTINCT:
		if (child->IsScalar()) {
			stats.has_stats = false;
			switch (child->return_type) {
			case TypeId::BOOLEAN:
			case TypeId::TINYINT:
			case TypeId::SMALLINT:
			case TypeId::INTEGER:
			case TypeId::BIGINT:
				return_type = TypeId::BIGINT;
				break;
			default:
				return_type = child->return_type;
			}
		} else {
			ExpressionStatistics::Count(child->stats, stats);
			ExpressionStatistics::Sum(child->stats, stats);
			return_type = max(child->return_type, stats.MinimalType());
		}

		break;
	case ExpressionType::AGGREGATE_FIRST:
		return_type = child->return_type;
		break;
	case ExpressionType::AGGREGATE_STDDEV_SAMP:
		return_type = TypeId::DECIMAL;
		break;
	default:
		throw NotImplementedException("Unsupported aggregate type!");
	}
}

unique_ptr<Expression> AggregateExpression::Copy() {
	auto new_child = child ? child->Copy() : nullptr;
	auto new_aggregate = make_unique<AggregateExpression>(type, move(new_child));
	new_aggregate->index = index;
	new_aggregate->CopyProperties(*this);
	return new_aggregate;
}

void AggregateExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.WriteOptional(child);
}

unique_ptr<Expression> AggregateExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto child = source.ReadOptional<Expression>();
	return make_unique<AggregateExpression>(type, move(child));
}

string AggregateExpression::GetName() const {
	if (!alias.empty()) {
		return alias;
	}
	switch (type) {
	case ExpressionType::AGGREGATE_COUNT:
	case ExpressionType::AGGREGATE_COUNT_STAR:
	case ExpressionType::AGGREGATE_COUNT_DISTINCT:
		return "COUNT";
	case ExpressionType::AGGREGATE_SUM:
	case ExpressionType::AGGREGATE_SUM_DISTINCT:
		return "SUM";
	case ExpressionType::AGGREGATE_MIN:
		return "MIN";
	case ExpressionType::AGGREGATE_MAX:
		return "MAX";
	case ExpressionType::AGGREGATE_FIRST:
		return "FIRST";
	case ExpressionType::AGGREGATE_STDDEV_SAMP:
		return "STDDEV_SAMP";
	default:
		return "UNKNOWN";
	}
}

void AggregateExpression::EnumerateChildren(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) {
	if (child) {
		child = callback(move(child));
	}
}

void AggregateExpression::EnumerateChildren(std::function<void(Expression* expression)> callback) const {
	if (child) {
		callback(child.get());
	}
}
