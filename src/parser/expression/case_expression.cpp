#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

string CaseExpression::ToString() const {
	return ToString<CaseExpression, ParsedExpression>(*this);
}

unique_ptr<ParsedExpression> CaseExpression::Copy() const {
	auto copy = make_uniq<CaseExpression>();
	copy->CopyProperties(*this);
	for (auto &check : case_checks) {
		CaseCheck new_check;
		new_check.when_expr = check.when_expr->Copy();
		new_check.then_expr = check.then_expr->Copy();
		copy->case_checks.push_back(std::move(new_check));
	}
	copy->else_expr = else_expr->Copy();
	return std::move(copy);
}

} // namespace duckdb
