#include "duckdb/parser/expression/comparison_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

namespace duckdb {

ComparisonExpression::ComparisonExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                           unique_ptr<ParsedExpression> right)
    : ParsedExpression(type, ExpressionClass::COMPARISON), left(move(left)), right(move(right)) {
}

string ComparisonExpression::ToString() const {
	return ToString<ComparisonExpression, ParsedExpression>(*this);
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

void ComparisonExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*left);
	writer.WriteSerializable(*right);
}

unique_ptr<ParsedExpression> ComparisonExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto left_child = reader.ReadRequiredSerializable<ParsedExpression>();
	auto right_child = reader.ReadRequiredSerializable<ParsedExpression>();
	return make_unique<ComparisonExpression>(type, move(left_child), move(right_child));
}

} // namespace duckdb
