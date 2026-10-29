#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

string CaseExpression::ToString() const {
	if (!case_operand) {
		return ToString<CaseExpression, ParsedExpression>(*this);
	}
	string case_str = "CASE (" + case_operand->ToString() + ")";
	for (auto &check : case_checks) {
		case_str += " WHEN (" + check.when_expr->ToString() + ")";
		case_str += " THEN (" + check.then_expr->ToString() + ")";
	}
	case_str += " ELSE " + else_expr->ToString();
	case_str += " END";
	return case_str;
}

unique_ptr<CaseExpression> CaseExpression::GetLegacyCaseExpression() const {
	D_ASSERT(case_operand);
	auto result = make_uniq<CaseExpression>();
	for (auto &check : case_checks) {
		CaseCheck legacy_check;
		auto operand_location = case_operand->GetQueryLocation();
		legacy_check.when_expr = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, case_operand->Copy(),
		                                                         check.when_expr->Copy());
		legacy_check.when_expr->SetQueryLocation(operand_location);
		legacy_check.then_expr = check.then_expr->Copy();
		result->CaseChecksMutable().push_back(std::move(legacy_check));
	}
	result->ElseMutable() = else_expr->Copy();
	result->SetAlias(GetAlias());
	result->SetQueryLocation(GetQueryLocation());
	return result;
}

namespace {

bool ContainsCaseExpression(const ParsedExpression &expr, ExpressionClass expression_class) {
	if (expr.GetExpressionClass() == expression_class &&
	    (expression_class != ExpressionClass::CASE || expr.Cast<CaseExpression>().CaseOperand())) {
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expr.Cast<SubqueryExpression>().Subquery();
		bool found = false;
		if (subquery) {
			ParsedExpressionIterator::EnumerateQueryNodeChildren(
			    *subquery->node, [&](unique_ptr<ParsedExpression> &child) {
				    found |= ContainsCaseExpression(*child, expression_class);
			    });
		}
		if (found) {
			return true;
		}
	}
	for (auto &child : expr.Children()) {
		if (ContainsCaseExpression(child, expression_class)) {
			return true;
		}
	}
	return false;
}

void NormalizeCaseExpression(unique_ptr<ParsedExpression> &expr) {
	if (expr->GetExpressionClass() == ExpressionClass::CASE && expr->Cast<CaseExpression>().CaseOperand()) {
		expr = expr->Cast<CaseExpression>().GetLegacyCaseExpression();
	}
	if (expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expr->Cast<SubqueryExpression>().Subquery();
		if (subquery) {
			ParsedExpressionIterator::EnumerateQueryNodeChildren(*subquery->node, NormalizeCaseExpression);
		}
	}
	for (auto &child : expr->ChildrenMutable()) {
		NormalizeCaseExpression(child);
	}
}

} // namespace

bool CaseExpression::ContainsSimpleCase(const ParsedExpression &expr) {
	return ContainsCaseExpression(expr, ExpressionClass::CASE);
}

bool CaseExpression::ContainsLambdaReference(const ParsedExpression &expr) {
	return ContainsCaseExpression(expr, ExpressionClass::LAMBDA_REF);
}

unique_ptr<ParsedExpression> CaseExpression::Normalize(const ParsedExpression &expr) {
	auto result = expr.Copy();
	NormalizeCaseExpression(result);
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
