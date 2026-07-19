#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

string CaseExpression::ToString() const {
	return ToString<CaseExpression, ParsedExpression>(*this);
}

} // namespace duckdb
