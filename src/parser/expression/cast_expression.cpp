#include "parser/expression/cast_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

CastExpression::CastExpression(SQLType target, unique_ptr<ParsedExpression> child) :
	ParsedExpression(ExpressionType::OPERATOR_CAST, ExpressionClass::CAST), cast_type(target) {
	assert(child);
	this->child = move(child);
}

string CastExpression::ToString() const {
	return "CAST[" + SQLTypeToString(cast_type) + "](" + child->ToString() + ")";
}

bool CastExpression::Equals(const ParsedExpression *other_) const {
	if (!ParsedExpression::Equals(other_)) {
		return false;
	}
	auto other = (CastExpression *)other_;
	if (!child->Equals(other->child.get())) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> CastExpression::Copy() {
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

size_t CastExpression::ChildCount() const {
	return 1;
}

ParsedExpression *CastExpression::GetChild(size_t index) const {
	assert(index == 0);
	return child.get();
}

void CastExpression::ReplaceChild(std::function<unique_ptr<ParsedExpression>(unique_ptr<ParsedExpression> expression)> callback,
                                  size_t index) {
	assert(index == 0);
	child = callback(move(child));
}
