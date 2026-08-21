#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"

namespace duckdb {

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

string CaseExpression::ToString() const {
	if (case_operand) {
		return GetLegacyCaseExpression()->ToString();
	}
	return ToString<CaseExpression, ParsedExpression>(*this);
}

unique_ptr<CaseExpression> CaseExpression::GetLegacyCaseExpression() const {
	D_ASSERT(case_operand);
	auto result = make_uniq<CaseExpression>();
	for (auto &check : case_checks) {
		CaseCheck legacy_check;
		legacy_check.when_expr = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, case_operand->Copy(),
		                                                         check.when_expr->Copy());
		legacy_check.then_expr = check.then_expr->Copy();
		result->CaseChecksMutable().push_back(std::move(legacy_check));
	}
	result->ElseMutable() = else_expr->Copy();
	result->SetAlias(GetAlias());
	result->SetQueryLocation(GetQueryLocation());
	return result;
}

bool CaseExpression::UseLegacySerialization() const {
	return case_operand != nullptr;
}

void CaseExpression::LegacySerialize(Serializer &serializer) const {
	auto legacy_case = GetLegacyCaseExpression();
	legacy_case->Serialize(serializer);
}

} // namespace duckdb
