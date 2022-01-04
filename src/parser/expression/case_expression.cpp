#include "duckdb/parser/expression/case_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

string CaseExpression::ToString() const {
	string case_str = "CASE ";
	for (auto &check : case_checks) {
		case_str += " WHEN (" + check.when_expr->ToString() + ")";
		case_str += " THEN (" + check.then_expr->ToString() + ")";
	}
	case_str += " ELSE " + else_expr->ToString();
	return case_str;
}

bool CaseExpression::Equals(const CaseExpression *a, const CaseExpression *b) {
	if (a->case_checks.size() != b->case_checks.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->case_checks.size(); i++) {
		if (!a->case_checks[i].when_expr->Equals(b->case_checks[i].when_expr.get())) {
			return false;
		}
		if (!a->case_checks[i].then_expr->Equals(b->case_checks[i].then_expr.get())) {
			return false;
		}
	}
	if (!a->else_expr->Equals(b->else_expr.get())) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> CaseExpression::Copy() const {
	auto copy = make_unique<CaseExpression>();
	copy->CopyProperties(*this);
	for (auto &check : case_checks) {
		CaseCheck new_check;
		new_check.when_expr = check.when_expr->Copy();
		new_check.then_expr = check.then_expr->Copy();
		copy->case_checks.push_back(move(new_check));
	}
	copy->else_expr = else_expr->Copy();
	return copy;
}

void CaseExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.Write<uint32_t>(case_checks.size());
	for (auto &check : case_checks) {
		check.when_expr->Serialize(serializer);
		check.then_expr->Serialize(serializer);
	}
	else_expr->Serialize(serializer);
}

unique_ptr<ParsedExpression> CaseExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto result = make_unique<CaseExpression>();
	auto count = source.Read<uint32_t>();
	for (idx_t i = 0; i < count; i++) {
		CaseCheck new_check;
		new_check.when_expr = ParsedExpression::Deserialize(source);
		new_check.then_expr = ParsedExpression::Deserialize(source);
		result->case_checks.push_back(move(new_check));
	}
	result->else_expr = ParsedExpression::Deserialize(source);
	return result;
}

} // namespace duckdb
