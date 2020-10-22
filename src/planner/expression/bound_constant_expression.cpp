#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/storage/statistics/statistics_operations.hpp"

namespace duckdb {
using namespace std;

BoundConstantExpression::BoundConstantExpression(Value value_p)
    : Expression(ExpressionType::VALUE_CONSTANT, ExpressionClass::BOUND_CONSTANT, value_p.type()), value(move(value_p)) {
	stats = StatisticsOperations::FromValue(value);
}

string BoundConstantExpression::ToString() const {
	return value.ToString();
}

bool BoundConstantExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundConstantExpression *)other_p;
	return value == other->value;
}

hash_t BoundConstantExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(ValueOperations::Hash(value), result);
}

unique_ptr<Expression> BoundConstantExpression::Copy() {
	auto copy = make_unique<BoundConstantExpression>(value);
	copy->CopyProperties(*this);
	return move(copy);
}

} // namespace duckdb
