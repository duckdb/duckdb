#include "duckdb/parser/expression/case_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"

namespace duckdb {

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

string CaseExpression::ToString() const {
	return ToString<CaseExpression, ParsedExpression>(*this);
}

static CaseCheck CopyCaseCheck(const CaseCheck &check) {
	CaseCheck result;
	result.when_expr = check.when_expr->Copy();
	result.then_expr = check.then_expr->Copy();
	return result;
}

vector<CaseCheck> CaseExpression::CaseChecksForSerialization(Serializer &serializer) const {
	vector<CaseCheck> result;
	result.reserve(case_checks.size());
	if (!case_expr || serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
		for (auto &check : case_checks) {
			result.push_back(CopyCaseCheck(check));
		}
		return result;
	}

	for (auto &check : case_checks) {
		CaseCheck legacy_check;
		legacy_check.when_expr =
		    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, case_expr->Copy(), check.when_expr->Copy());
		legacy_check.then_expr = check.then_expr->Copy();
		result.push_back(std::move(legacy_check));
	}
	return result;
}

} // namespace duckdb
