#include "duckdb/parser/expression/comparison_expression.hpp"

namespace duckdb {

ComparisonExpression::ComparisonExpression(ExpressionType type) : ParsedExpression(type, ExpressionClass::COMPARISON) {
}

ComparisonExpression::ComparisonExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                           unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::COMPARISON), left(std::move(left)), right(std::move(right)) {
}

string ComparisonExpression::ToString() const {
	return ToString<ParsedExpression>(type, *left, *right);
}

unique_ptr<ParsedExpression> ComparisonExpression::Copy() const {
	auto copy = make_uniq<ComparisonExpression>(type, left->Copy(), right->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
