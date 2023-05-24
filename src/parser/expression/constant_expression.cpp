#include "duckdb/parser/expression/constant_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

ConstantExpression::ConstantExpression(Value val)
    : ParsedExpression(ExpressionType::VALUE_CONSTANT, ExpressionClass::CONSTANT), value(std::move(val)) {
}

string ConstantExpression::ToString() const {
	return value.ToSQLString();
}

bool ConstantExpression::Equal(const ConstantExpression &a, const ConstantExpression &b) {
	return a.value.type() == b.value.type() && !ValueOperations::DistinctFrom(a.value, b.value);
}

hash_t ConstantExpression::Hash() const {
	return value.Hash();
}

unique_ptr<ParsedExpression> ConstantExpression::Copy() const {
	auto copy = make_uniq<ConstantExpression>(value);
	copy->CopyProperties(*this);
	return std::move(copy);
}

void ConstantExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(value);
}

unique_ptr<ParsedExpression> ConstantExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	Value value = reader.ReadRequiredSerializable<Value, Value>();
	return make_uniq<ConstantExpression>(std::move(value));
}

void ConstantExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("value", value);
}

unique_ptr<ParsedExpression> ConstantExpression::FormatDeserialize(ExpressionType type,
                                                                   FormatDeserializer &deserializer) {
	auto value = deserializer.ReadProperty<Value>("value");
	return make_uniq<ConstantExpression>(std::move(value));
}

} // namespace duckdb
