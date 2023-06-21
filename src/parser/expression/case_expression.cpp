#include "duckdb/parser/expression/case_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

void CaseCheck::FormatSerialize(FormatSerializer &serializer) const {
	serializer.WriteProperty("when_expr", when_expr);
	serializer.WriteProperty("then_expr", then_expr);
}

CaseCheck CaseCheck::FormatDeserialize(FormatDeserializer &deserializer) {
	CaseCheck check;
	deserializer.ReadProperty("when_expr", check.when_expr);
	deserializer.ReadProperty("then_expr", check.then_expr);
	return check;
}

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
	auto result = make_uniq<CaseExpression>();
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

void CaseExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("case_checks", case_checks);
	serializer.WriteProperty("else_expr", *else_expr);
}

unique_ptr<ParsedExpression> CaseExpression::FormatDeserialize(ExpressionType type, FormatDeserializer &deserializer) {
	auto result = make_uniq<CaseExpression>();
	deserializer.ReadProperty("case_checks", result->case_checks);
	deserializer.ReadProperty("else_expr", result->else_expr);
	return std::move(result);
}

} // namespace duckdb
