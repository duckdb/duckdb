#include "duckdb/parser/expression/conjunction_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

using namespace duckdb;
using namespace std;

ConjunctionExpression::ConjunctionExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                             unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::CONJUNCTION), left(move(left)), right(move(right)) {
}

string ConjunctionExpression::ToString() const {
	return left->ToString() + " " + ExpressionTypeToOperator(type) + " " + right->ToString();
}

bool ConjunctionExpression::Equals(const ConjunctionExpression *a, const ConjunctionExpression *b) {
	// conjunctions are Commutative
	if (a->left->Equals(b->left.get()) && a->right->Equals(b->right.get())) {
		return true;
	}
	if (a->right->Equals(b->left.get()) && a->left->Equals(b->right.get())) {
		return true;
	}
	return false;
}

unique_ptr<ParsedExpression> ConjunctionExpression::Copy() const {
	auto copy = make_unique<ConjunctionExpression>(type, left->Copy(), right->Copy());
	copy->CopyProperties(*this);
	return move(copy);
}

void ConjunctionExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	left->Serialize(serializer);
	right->Serialize(serializer);
}

unique_ptr<ParsedExpression> ConjunctionExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto left_child = ParsedExpression::Deserialize(source);
	auto right_child = ParsedExpression::Deserialize(source);
	return make_unique<ConjunctionExpression>(type, move(left_child), move(right_child));
}
