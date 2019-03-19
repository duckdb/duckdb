#include "parser/expression/constant_expression.hpp"

#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ConstantExpression::Copy() const {
	auto copy = make_unique<ConstantExpression>(value);
	copy->CopyProperties(*this);
	return move(copy);
}

void ConstantExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	value.Serialize(serializer);
}

unique_ptr<Expression> ConstantExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	Value value = Value::Deserialize(source);
	auto expression = make_unique_base<Expression, ConstantExpression>(value);
	return expression;
}

void ConstantExpression::ResolveType() {
	Expression::ResolveType();
	stats.SetFromValue(value);
}

bool ConstantExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (ConstantExpression *)other_;
	return value == other->value;
}

uint64_t ConstantExpression::Hash() const {
	uint64_t result = Expression::Hash();
	return CombineHash(ValueOperations::Hash(value), result);
}
