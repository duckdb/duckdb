#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"

namespace duckdb {

BoundComparisonExpression::BoundComparisonExpression(ExpressionType type, unique_ptr<Expression> left,
                                                     unique_ptr<Expression> right)
    : Expression(type, ExpressionClass::BOUND_COMPARISON, LogicalType::BOOLEAN), left(std::move(left)),
      right(std::move(right)) {
}

string BoundComparisonExpression::ToString() const {
	return ComparisonExpression::ToString<BoundComparisonExpression, Expression>(*this);
}

bool BoundComparisonExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundComparisonExpression *)other_p;
	if (!Expression::Equals(left.get(), other->left.get())) {
		return false;
	}
	if (!Expression::Equals(right.get(), other->right.get())) {
		return false;
	}

	return true;
}

unique_ptr<Expression> BoundComparisonExpression::Copy() {
	auto copy = make_unique<BoundComparisonExpression>(type, left->Copy(), right->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
}

void BoundComparisonExpression::Serialize(FieldWriter &writer) const {
	writer.WriteOptional(left);
	writer.WriteOptional(right);
}

unique_ptr<Expression> BoundComparisonExpression::Deserialize(ExpressionDeserializationState &state,
                                                              FieldReader &reader) {
	auto left = reader.ReadOptional<Expression>(nullptr, state.gstate);
	auto right = reader.ReadOptional<Expression>(nullptr, state.gstate);
	return make_unique<BoundComparisonExpression>(state.type, std::move(left), std::move(right));
}

} // namespace duckdb
