#include "duckdb/parser/expression/cast_expression.hpp"

#include "duckdb/common/exception.hpp"

using namespace duckdb;
using namespace std;

CastExpression::CastExpression(SQLType target, unique_ptr<ParsedExpression> child)
    : ParsedExpression(ExpressionType::OPERATOR_CAST, ExpressionClass::CAST), cast_type(target) {
	assert(child);
	this->child = move(child);
}

string CastExpression::ToString() const {
	return "CAST[" + SQLTypeToString(cast_type) + "](" + child->ToString() + ")";
}

bool CastExpression::Equals(const CastExpression *a, const CastExpression *b) {
	if (!a->child->Equals(b->child.get())) {
		return false;
	}
	if (a->cast_type != b->cast_type) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> CastExpression::Copy() const {
	auto copy = make_unique<CastExpression>(cast_type, child->Copy());
	copy->CopyProperties(*this);
	return move(copy);
}

void CastExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	child->Serialize(serializer);
	cast_type.Serialize(serializer);
}

unique_ptr<ParsedExpression> CastExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto child = ParsedExpression::Deserialize(source);
	auto cast_type = SQLType::Deserialize(source);
	return make_unique_base<ParsedExpression, CastExpression>(cast_type, move(child));
}
