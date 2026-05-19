#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression_util.hpp"

namespace duckdb {

ConjunctionExpression::ConjunctionExpression()
    : ParsedExpression(ExpressionType::INVALID, ExpressionClass::CONJUNCTION) {
}

ConjunctionExpression::ConjunctionExpression(ExpressionType type)
    : ParsedExpression(type, ExpressionClass::CONJUNCTION) {
}

ConjunctionExpression::ConjunctionExpression(ExpressionType type, vector<unique_ptr<ParsedExpression>> children)
    : ParsedExpression(type, ExpressionClass::CONJUNCTION) {
	for (auto &child : children) {
		AddExpression(std::move(child));
	}
}

ConjunctionExpression::ConjunctionExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                             unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::CONJUNCTION) {
	AddExpression(std::move(left));
	AddExpression(std::move(right));
}

void ConjunctionExpression::AddExpression(unique_ptr<ParsedExpression> expr) {
	if (expr->GetExpressionType() == type) {
		// expr is a conjunction of the same type: merge the expression lists together
		auto &other = expr->Cast<ConjunctionExpression>();
		for (auto &child : other.children) {
			children.push_back(std::move(child));
		}
	} else {
		children.push_back(std::move(expr));
	}
}

string ConjunctionExpression::ToString() const {
	return ToString<ConjunctionExpression, ParsedExpression>(*this);
}

} // namespace duckdb
