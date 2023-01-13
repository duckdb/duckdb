#include "duckdb/parser/expression/case_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

string CaseExpression::ToString() const {
	return ToString<CaseExpression, ParsedExpression>(*this);
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
		copy->case_checks.push_back(std::move(new_check));
	}
	copy->else_expr = else_expr->Copy();
	return std::move(copy);
}

void CaseExpression::Serialize(FieldWriter &writer) const {
	auto &serializer = writer.GetSerializer();
	// we write a list of multiple expressions here
	// in order to write this as a single field we directly use the field writers' internal serializer
	writer.WriteField<uint32_t>(case_checks.size());
	for (auto &check : case_checks) {
		check.when_expr->Serialize(serializer);
		check.then_expr->Serialize(serializer);
	}
	writer.WriteSerializable<ParsedExpression>(*else_expr);
}

unique_ptr<ParsedExpression> CaseExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto result = make_unique<CaseExpression>();
	auto &source = reader.GetSource();
	auto count = reader.ReadRequired<uint32_t>();
	for (idx_t i = 0; i < count; i++) {
		CaseCheck new_check;
		new_check.when_expr = ParsedExpression::Deserialize(source);
		new_check.then_expr = ParsedExpression::Deserialize(source);
		result->case_checks.push_back(std::move(new_check));
	}
	result->else_expr = reader.ReadRequiredSerializable<ParsedExpression>();
	return std::move(result);
}

} // namespace duckdb
