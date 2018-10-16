
#include "parser/expression/constant_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ConstantExpression::Copy() {
	assert(children.size() == 0);
	auto copy = make_unique<ConstantExpression>(value);
	copy->CopyProperties(*this);
	return copy;
}

void ConstantExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	value.Serialize(serializer);
}

unique_ptr<Expression>
ConstantExpression::Deserialize(ExpressionDeserializeInformation *info,
                                Deserializer &source) {
	Value value = Value::Deserialize(source);
	auto expression = make_unique_base<Expression, ConstantExpression>(value);
	expression->children = move(info->children);
	return expression;
}

void ConstantExpression::ResolveType() {
	Expression::ResolveType();
	stats = Statistics(value);
}

bool ConstantExpression::Equals(const Expression *other_) {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = reinterpret_cast<const ConstantExpression *>(other_);
	if (!other) {
		return false;
	}
	return Value::Equals(value, other->value);
}
