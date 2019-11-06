#include "duckdb/parser/expression/case_expression.hpp"

#include "duckdb/common/exception.hpp"

using namespace duckdb;
using namespace std;

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

string CaseExpression::ToString() const {
	return "CASE WHEN (" + check->ToString() + ") THEN (" + result_if_true->ToString() + ") ELSE (" +
	       result_if_false->ToString() + ")";
}

bool CaseExpression::Equals(const CaseExpression *a, const CaseExpression *b) {
	if (!a->check->Equals(b->check.get())) {
		return false;
	}
	if (!a->result_if_true->Equals(b->result_if_true.get())) {
		return false;
	}
	if (!a->result_if_false->Equals(b->result_if_false.get())) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> CaseExpression::Copy() const {
	auto copy = make_unique<CaseExpression>();
	copy->CopyProperties(*this);
	copy->check = check->Copy();
	copy->result_if_true = result_if_true->Copy();
	copy->result_if_false = result_if_false->Copy();
	return move(copy);
}

void CaseExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	check->Serialize(serializer);
	result_if_true->Serialize(serializer);
	result_if_false->Serialize(serializer);
}

unique_ptr<ParsedExpression> CaseExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto expression = make_unique<CaseExpression>();
	expression->check = ParsedExpression::Deserialize(source);
	expression->result_if_true = ParsedExpression::Deserialize(source);
	expression->result_if_false = ParsedExpression::Deserialize(source);
	return move(expression);
}
