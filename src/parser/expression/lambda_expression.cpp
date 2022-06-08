#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression(unique_ptr<ParsedExpression> lhs, unique_ptr<ParsedExpression> expr)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), lhs(move(lhs)), expr(move(expr)) {
}

string LambdaExpression::ToString() const {
	return lhs->ToString() + " -> " + expr->ToString();
}

bool LambdaExpression::Equals(const LambdaExpression *a, const LambdaExpression *b) {
	return a->lhs->Equals(b->lhs.get()) && a->expr->Equals(b->expr.get());
}

hash_t LambdaExpression::Hash() const {

	hash_t result = lhs->Hash();
	ParsedExpression::Hash();
	result = CombineHash(result, expr->Hash());
	return result;
}

unique_ptr<ParsedExpression> LambdaExpression::Copy() const {
	return make_unique<LambdaExpression>(lhs->Copy(), expr->Copy());
}

void LambdaExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*lhs);
	writer.WriteSerializable(*expr);
}

unique_ptr<ParsedExpression> LambdaExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto lhs = reader.ReadRequiredSerializable<ParsedExpression>();
	auto expr = reader.ReadRequiredSerializable<ParsedExpression>();
	return make_unique<LambdaExpression>(move(lhs), move(expr));
}

} // namespace duckdb
