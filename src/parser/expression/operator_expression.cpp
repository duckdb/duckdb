#include "duckdb/parser/expression/operator_expression.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

OperatorExpression::OperatorExpression() : ParsedExpression(ExpressionType::INVALID, ExpressionClass::OPERATOR) {
}

OperatorExpression::OperatorExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                       unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::OPERATOR) {
	if (left) {
		children.push_back(std::move(left));
	}
	if (right) {
		children.push_back(std::move(right));
	}
}

OperatorExpression::OperatorExpression(ExpressionType type, vector<unique_ptr<ParsedExpression>> children)
    : ParsedExpression(type, ExpressionClass::OPERATOR), children(std::move(children)) {
}

string OperatorExpression::ToString() const {
	return ToString<OperatorExpression, ParsedExpression>(*this);
}

} // namespace duckdb
