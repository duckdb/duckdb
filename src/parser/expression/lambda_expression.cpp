#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression(unique_ptr<ParsedExpression> lhs, unique_ptr<ParsedExpression> rhs)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), lhs(move(lhs)), rhs(move(rhs)) {
}

string LambdaExpression::ToString() const {
	return lhs->ToString() + " -> " + rhs->ToString();
}

bool LambdaExpression::Equals(const LambdaExpression *a, const LambdaExpression *b) {
	return a->lhs->Equals(b->lhs.get()) && a->rhs->Equals(b->rhs.get());
}

hash_t LambdaExpression::Hash() const {
	hash_t result = lhs->Hash();
	ParsedExpression::Hash();
	result = CombineHash(result, rhs->Hash());
	return result;
}

unique_ptr<ParsedExpression> LambdaExpression::Copy() const {
	return make_unique<LambdaExpression>(lhs->Copy(), rhs->Copy());
}

void LambdaExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*lhs);
	writer.WriteSerializable(*rhs);
}

unique_ptr<ParsedExpression> LambdaExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto lhs = reader.ReadRequiredSerializable<ParsedExpression>();
	auto rhs = reader.ReadRequiredSerializable<ParsedExpression>();
	return make_unique<LambdaExpression>(move(lhs), move(rhs));
}

} // namespace duckdb
