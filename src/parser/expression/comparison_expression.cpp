#include "duckdb/parser/expression/comparison_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

using namespace duckdb;
using namespace std;

ComparisonExpression::ComparisonExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                           unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::COMPARISON) {
	this->left = move(left);
	this->right = move(right);
}

string ComparisonExpression::ToString() const {
	return left->ToString() + ExpressionTypeToOperator(type) + right->ToString();
}

bool ComparisonExpression::Equals(const ComparisonExpression *a, const ComparisonExpression *b) {
	if (!a->left->Equals(b->left.get())) {
		return false;
	}
	if (!a->right->Equals(b->right.get())) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> ComparisonExpression::Copy() const {
	auto copy = make_unique<ComparisonExpression>(type, left->Copy(), right->Copy());
	copy->CopyProperties(*this);
	return move(copy);
}

void ComparisonExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	left->Serialize(serializer);
	right->Serialize(serializer);
}

unique_ptr<ParsedExpression> ComparisonExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto left_child = ParsedExpression::Deserialize(source);
	auto right_child = ParsedExpression::Deserialize(source);
	return make_unique<ComparisonExpression>(type, move(left_child), move(right_child));
}
