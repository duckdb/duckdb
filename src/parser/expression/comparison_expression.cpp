#include "duckdb/parser/expression/comparison_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

ComparisonExpression::ComparisonExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                           unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::COMPARISON), left(std::move(left)), right(std::move(right)) {
}

string ComparisonExpression::ToString() const {
	return ToString<ComparisonExpression, ParsedExpression>(*this);
}

bool ComparisonExpression::Equal(const ComparisonExpression &a, const ComparisonExpression &b) {
	if (!a.left->Equals(*b.left)) {
		return false;
	}
	if (!a.right->Equals(*b.right)) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> ComparisonExpression::Copy() const {
	auto copy = make_uniq<ComparisonExpression>(type, left->Copy(), right->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
}

void ComparisonExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*left);
	writer.WriteSerializable(*right);
}

unique_ptr<ParsedExpression> ComparisonExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto left_child = reader.ReadRequiredSerializable<ParsedExpression>();
	auto right_child = reader.ReadRequiredSerializable<ParsedExpression>();
	return make_uniq<ComparisonExpression>(type, std::move(left_child), std::move(right_child));
}

void ComparisonExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("left", *left);
	serializer.WriteProperty("right", *right);
}

unique_ptr<ParsedExpression> ComparisonExpression::FormatDeserialize(ExpressionType type,
                                                                     FormatDeserializer &deserializer) {
	auto left = deserializer.ReadProperty<unique_ptr<ParsedExpression>>("left");
	auto right = deserializer.ReadProperty<unique_ptr<ParsedExpression>>("right");
	return make_uniq<ComparisonExpression>(type, std::move(left), std::move(right));
}

} // namespace duckdb
