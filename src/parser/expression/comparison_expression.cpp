#include "parser/expression/comparison_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ComparisonExpression::Copy() {
	auto copy = make_unique<ComparisonExpression>(type, left->Copy(), right->Copy());
	copy->CopyProperties(*this);
	return copy;
}

void ComparisonExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	left->Serialize(serializer);
	right->Serialize(serializer);
}

unique_ptr<Expression> ComparisonExpression::Deserialize(ExpressionType type, TypeId return_type,
                                                         Deserializer &source) {
	auto left_child = Expression::Deserialize(source);
	auto right_child = Expression::Deserialize(source);
	return make_unique<ComparisonExpression>(type, move(left_child), move(right_child));
}

bool ComparisonExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (ComparisonExpression *)other_;
	if (!left->Equals(other->left.get())) {
		return false;
	}
	if (!right->Equals(other->right.get())) {
		return false;
	}
	return true;
}

void ComparisonExpression::EnumerateChildren(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) {
	left = callback(move(left));
	right = callback(move(right));
}

void ComparisonExpression::EnumerateChildren(std::function<void(Expression *expression)> callback) const {
	callback(left.get());
	callback(right.get());
}

ExpressionType ComparisonExpression::NegateComparisionExpression(ExpressionType type) {
	ExpressionType negated_type = ExpressionType::INVALID;
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		negated_type = ExpressionType::COMPARE_NOTEQUAL;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		negated_type = ExpressionType::COMPARE_EQUAL;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		negated_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		negated_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		negated_type = ExpressionType::COMPARE_GREATERTHAN;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		negated_type = ExpressionType::COMPARE_LESSTHAN;
		break;

	default:
		throw Exception("Unsupported join criteria in negation");
	}
	return negated_type;
}

ExpressionType ComparisonExpression::FlipComparisionExpression(ExpressionType type) {
	ExpressionType flipped_type = ExpressionType::INVALID;
	switch (type) {
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_EQUAL:
		flipped_type = type;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		flipped_type = ExpressionType::COMPARE_GREATERTHAN;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		flipped_type = ExpressionType::COMPARE_LESSTHAN;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		flipped_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		flipped_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;

	default:
		throw Exception("Unsupported join criteria in flip");
	}
	return flipped_type;
}
