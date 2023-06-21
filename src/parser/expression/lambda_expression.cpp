#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression(unique_ptr<ParsedExpression> lhs, unique_ptr<ParsedExpression> expr)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), lhs(std::move(lhs)), expr(std::move(expr)) {
}

string LambdaExpression::ToString() const {
	return lhs->ToString() + " -> " + expr->ToString();
}

bool LambdaExpression::Equal(const LambdaExpression &a, const LambdaExpression &b) {
	return a.lhs->Equals(*b.lhs) && a.expr->Equals(*b.expr);
}

hash_t LambdaExpression::Hash() const {
	hash_t result = lhs->Hash();
	ParsedExpression::Hash();
	result = CombineHash(result, expr->Hash());
	return result;
}

unique_ptr<ParsedExpression> LambdaExpression::Copy() const {
	auto copy = make_uniq<LambdaExpression>(lhs->Copy(), expr->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
}

void LambdaExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*lhs);
	writer.WriteSerializable(*expr);
}

unique_ptr<ParsedExpression> LambdaExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto lhs = reader.ReadRequiredSerializable<ParsedExpression>();
	auto expr = reader.ReadRequiredSerializable<ParsedExpression>();
	return make_uniq<LambdaExpression>(std::move(lhs), std::move(expr));
}

void LambdaExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("lhs", *lhs);
	serializer.WriteProperty("expr", *expr);
}

unique_ptr<ParsedExpression> LambdaExpression::FormatDeserialize(ExpressionType type,
                                                                 FormatDeserializer &deserializer) {
	auto lhs = deserializer.ReadProperty<unique_ptr<ParsedExpression>>("lhs");
	auto expr = deserializer.ReadProperty<unique_ptr<ParsedExpression>>("expr");
	return make_uniq<LambdaExpression>(std::move(lhs), std::move(expr));
}

} // namespace duckdb
