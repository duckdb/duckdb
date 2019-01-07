#include "parser/expression/conjunction_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ConjunctionExpression::Copy() {
	auto copy = make_unique<ConjunctionExpression>(type, left->Copy(), right->Copy());
	copy->CopyProperties(*this);
	return copy;
}

void ConjunctionExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	left->Serialize(serializer);
	right->Serialize(serializer);
}

unique_ptr<Expression> ConjunctionExpression::Deserialize(ExpressionType type, TypeId return_type,
                                                          Deserializer &source) {
	auto left_child = Expression::Deserialize(source);
	auto right_child = Expression::Deserialize(source);
	return make_unique<ConjunctionExpression>(type, move(left_child), move(right_child));
}

bool ConjunctionExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (ConjunctionExpression *)other_;
	// conjunctions are Commutative
	if (left->Equals(other->left.get()) && right->Equals(other->right.get())) {
		return true;
	}
	if (right->Equals(other->left.get()) && left->Equals(other->right.get())) {
		return true;
	}
	return false;
}

void ConjunctionExpression::EnumerateChildren(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) {
	left = callback(move(left));
	right = callback(move(right));
}

void ConjunctionExpression::EnumerateChildren(std::function<void(Expression *expression)> callback) const {
	callback(left.get());
	callback(right.get());
}
