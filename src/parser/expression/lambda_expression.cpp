#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

LambdaExpression::LambdaExpression() : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA) {
}

LambdaExpression::LambdaExpression(unique_ptr<ParsedExpression> lhs, unique_ptr<ParsedExpression> expr)
    : ParsedExpression(ExpressionType::LAMBDA, ExpressionClass::LAMBDA), lhs(std::move(lhs)), expr(std::move(expr)) {
}

string LambdaExpression::ToString() const {
	return "(" + lhs->ToString() + " -> " + expr->ToString() + ")";
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

} // namespace duckdb
