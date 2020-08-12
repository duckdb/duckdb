#include "duckdb/parser/expression/constant_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

namespace duckdb {
using namespace std;

ConstantExpression::ConstantExpression(LogicalType sql_type, Value val)
    : ParsedExpression(ExpressionType::VALUE_CONSTANT, ExpressionClass::CONSTANT), value(val), sql_type(sql_type) {
}

string ConstantExpression::ToString() const {
	return value.ToString();
}

bool ConstantExpression::Equals(const ConstantExpression *a, const ConstantExpression *b) {
	return a->value == b->value;
}

hash_t ConstantExpression::Hash() const {
	return ParsedExpression::Hash();
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
	auto sql_type = LogicalType::Deserialize(source);
	return make_unique<ConstantExpression>(sql_type, value);
}

} // namespace duckdb
