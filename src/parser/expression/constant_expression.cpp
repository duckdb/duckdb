#include "parser/expression/constant_expression.hpp"

#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

ConstantExpression::ConstantExpression(SQLType sql_type, Value val)
    : ParsedExpression(ExpressionType::VALUE_CONSTANT, ExpressionClass::CONSTANT), value(val), sql_type(sql_type) {
}

string ConstantExpression::ToString() const {
	return value.ToString();
}

bool ConstantExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (ConstantExpression *)other_;
	return value == other->value;
}

uint64_t ConstantExpression::Hash() const {
	uint64_t result = ParsedExpression::Hash();
	return CombineHash(ValueOperations::Hash(value), result);
}

unique_ptr<ParsedExpression> ConstantExpression::Copy() const {
	auto copy = make_unique<ConstantExpression>(sql_type, value);
	copy->CopyProperties(*this);
	return move(copy);
}

void ConstantExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	value.Serialize(serializer);
	sql_type.Serialize(serializer);
}

unique_ptr<ParsedExpression> ConstantExpression::Deserialize(ExpressionType type, Deserializer &source) {
	Value value = Value::Deserialize(source);
	auto sql_type = SQLType::Deserialize(source);
	return make_unique<ConstantExpression>(sql_type, value);
}
