#include "duckdb/planner/expression/bound_comparison_expression.hpp"

using namespace duckdb;
using namespace std;

BoundComparisonExpression::BoundComparisonExpression(ExpressionType type, unique_ptr<Expression> left,
                                                     unique_ptr<Expression> right)
    : Expression(type, ExpressionClass::BOUND_COMPARISON, TypeId::BOOL), left(move(left)), right(move(right)) {
}

string BoundComparisonExpression::ToString() const {
	return left->GetName() + ExpressionTypeToOperator(type) + right->GetName();
}

bool BoundComparisonExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundComparisonExpression *)other_;
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
	return move(copy);
}
