#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type, unique_ptr<Expression> left,
                                                       unique_ptr<Expression> right)
    : Expression(type, ExpressionClass::BOUND_CONJUNCTION, TypeId::BOOLEAN), left(move(left)), right(move(right)) {
}

string BoundConjunctionExpression::ToString() const {
	return left->GetName() + " " + ExpressionTypeToOperator(type) + " " + right->GetName();
}

bool BoundConjunctionExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundConjunctionExpression *)other_;
	// conjunctions are commutative
	if (Expression::Equals(left.get(), other->left.get()) && Expression::Equals(right.get(), other->right.get())) {
		return true;
	}
	if (Expression::Equals(left.get(), other->right.get()) && Expression::Equals(right.get(), other->left.get())) {
		return true;
	}
	return false;
}

unique_ptr<Expression> BoundConjunctionExpression::Copy() {
	auto copy = make_unique<BoundConjunctionExpression>(type, left->Copy(), right->Copy());
	copy->CopyProperties(*this);
	return move(copy);
}
