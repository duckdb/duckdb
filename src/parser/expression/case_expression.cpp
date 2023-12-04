#include "duckdb/parser/expression/case_expression.hpp"

#include "duckdb/common/exception.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

string CaseExpression::ToString() const {
	return ToString<CaseExpression, ParsedExpression>(*this);
}

bool CaseExpression::Equal(const CaseExpression &a, const CaseExpression &b) {
	if (a.case_checks.size() != b.case_checks.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.case_checks.size(); i++) {
		if (!a.case_checks[i].when_expr->Equals(*b.case_checks[i].when_expr)) {
			return false;
		}
		if (!a.case_checks[i].then_expr->Equals(*b.case_checks[i].then_expr)) {
			return false;
		}
	}
	if (!a.else_expr->Equals(*b.else_expr)) {
		return false;
	}
	return true;
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
