#include "duckdb/parser/expression/comparison_expression.hpp"

namespace duckdb {

ComparisonExpression::ComparisonExpression() : ParsedExpression(ExpressionType::INVALID, ExpressionClass::COMPARISON) {
}

ComparisonExpression::ComparisonExpression(ExpressionType type) : ParsedExpression(type, ExpressionClass::COMPARISON) {
}

ComparisonExpression::ComparisonExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                           unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::COMPARISON), left(std::move(left)), right(std::move(right)) {
}

string ComparisonExpression::ToString() const {
	return ToString<ParsedExpression>(type, Left(), Right());
}

} // namespace duckdb
