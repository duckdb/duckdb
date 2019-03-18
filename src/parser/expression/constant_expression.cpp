#include "parser/expression/constant_expression.hpp"

#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

ConstantExpression::ConstantExpression(Value val) :
	ParsedExpression(ExpressionType::VALUE_CONSTANT, ExpressionClass::CONSTANT), value(val) {
}

string ConstantExpression::ToString() const {
	return value.ToString();
}

bool ConstantExpression::Equals(const ParsedExpression *other_) const {
	if (!ParsedExpression::Equals(other_)) {
		return false;
	}
	auto other = (ConstantExpression *)other_;
	return value == other->value;
}

uint64_t ConstantExpression::Hash() const {
	uint64_t result = ParsedExpression::Hash();
	return CombineHash(ValueOperations::Hash(value), result);
}

unique_ptr<ParsedExpression> ConstantExpression::Copy() {
	auto copy = make_unique<ConstantExpression>(value);
	copy->CopyProperties(*this);
	return move(copy);
}

void ConstantExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	value.Serialize(serializer);
}

unique_ptr<ParsedExpression> ConstantExpression::Deserialize(ExpressionType type, Deserializer &source) {
	Value value = Value::Deserialize(source);
	auto expression = make_unique_base<ParsedExpression, ConstantExpression>(value);
	return expression;
}
