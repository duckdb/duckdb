#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

namespace duckdb {

BoundConstantExpression::BoundConstantExpression(Value value_p)
    : Expression(ExpressionType::VALUE_CONSTANT, ExpressionClass::BOUND_CONSTANT, value_p.type()),
      value(std::move(value_p)) {
}

string BoundConstantExpression::ToString() const {
	return value.ToSQLString();
}

bool BoundConstantExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundConstantExpression>();
	return value.type() == other.value.type() && !ValueOperations::DistinctFrom(value, other.value);
}

hash_t BoundConstantExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(value.Hash(), result);
}

unique_ptr<Expression> BoundConstantExpression::Copy() const {
	auto copy = make_uniq<BoundConstantExpression>(value);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
