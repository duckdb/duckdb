#include "duckdb/parser/expression/collate_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

CollateExpression::CollateExpression(string collation, unique_ptr<ParsedExpression> child)
    : ParsedExpression(ExpressionType::COLLATE, ExpressionClass::COLLATE), collation(collation) {
	assert(child);
	this->child = move(child);
}

string CollateExpression::ToString() const {
	return "COLLATE(" + child->ToString() + ")";
}

bool CollateExpression::Equals(const CollateExpression *a, const CollateExpression *b) {
	if (!a->child->Equals(b->child.get())) {
		return false;
	}
	if (a->collation != b->collation) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> CollateExpression::Copy() const {
	auto copy = make_unique<CollateExpression>(collation, child->Copy());
	copy->CopyProperties(*this);
	return move(copy);
}

void CollateExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	child->Serialize(serializer);
	serializer.WriteString(collation);
}

unique_ptr<ParsedExpression> CollateExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto child = ParsedExpression::Deserialize(source);
	auto collation = source.Read<string>();
	return make_unique_base<ParsedExpression, CollateExpression>(collation, move(child));
}

} // namespace duckdb
