#include "parser/expression/aggregate_expression.hpp"

#include "common/serializer.hpp"
#include "parser/expression/cast_expression.hpp"

using namespace duckdb;
using namespace std;

AggregateExpression::AggregateExpression(ExpressionType type, unique_ptr<Expression> child) : Expression(type) {

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
	switch (type) {
	case ExpressionType::AGGREGATE_COUNT_STAR:
	case ExpressionType::AGGREGATE_COUNT:
	case ExpressionType::AGGREGATE_COUNT_DISTINCT:
		// for the COUNT() operators we don't have to cast the child
		// as the result depends only on the nullmask
		// hence we return here
		return;
	default:
		// for the other aggregates we have to cast the child
		child = CastExpression::AddCastToType(return_type, move(child));
	}
}

unique_ptr<Expression> AggregateExpression::Copy() const {
	auto new_child = child ? child->Copy() : nullptr;
	auto new_aggregate = make_unique<AggregateExpression>(type, move(new_child));
	new_aggregate->CopyProperties(*this);
	return move(new_aggregate);
}

void AggregateExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.WriteOptional(child);
}

unique_ptr<Expression> AggregateExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto child = source.ReadOptional<Expression>();
	return make_unique<AggregateExpression>(type, move(child));
}

bool AggregateExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
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

size_t AggregateExpression::ChildCount() const {
	return child ? 1 : 0;
}

Expression *AggregateExpression::GetChild(size_t index) const {
	assert(index == 0);
	return child.get();
}

void AggregateExpression::ReplaceChild(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback, size_t index) {
	assert(index == 0);
	child = callback(move(child));
}
