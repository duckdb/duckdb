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
	vector<unique_ptr<ParsedExpression>> case_operands;
	case_operands.reserve(case_checks.size());
	for (idx_t i = 0; i < case_checks.size(); i++) {
		case_operands.push_back(case_operand->Copy());
	}
	return GetLegacyCaseExpression(std::move(case_operands));
}

unique_ptr<CaseExpression>
CaseExpression::GetLegacyCaseExpression(vector<unique_ptr<ParsedExpression>> case_operands) const {
	D_ASSERT(case_operands.size() == case_checks.size());
	auto result = make_uniq<CaseExpression>();
	for (idx_t i = 0; i < case_checks.size(); i++) {
		auto &check = case_checks[i];
		CaseCheck legacy_check;
		auto operand_location = case_operands[i]->GetQueryLocation();
		legacy_check.when_expr = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
		                                                         std::move(case_operands[i]), check.when_expr->Copy());
		legacy_check.when_expr->SetQueryLocation(operand_location);
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
