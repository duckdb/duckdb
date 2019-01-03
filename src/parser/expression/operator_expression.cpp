#include "parser/expression/operator_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

void OperatorExpression::ResolveType() {
	Expression::ResolveType();
	// logical operators return a bool
	if (type == ExpressionType::OPERATOR_NOT || type == ExpressionType::OPERATOR_IS_NULL ||
	    type == ExpressionType::OPERATOR_IS_NOT_NULL || type == ExpressionType::OPERATOR_EXISTS ||
	    type == ExpressionType::OPERATOR_NOT_EXISTS || type == ExpressionType::COMPARE_IN ||
	    type == ExpressionType::COMPARE_NOT_IN) {
		return_type = TypeId::BOOLEAN;
		return;
	}
	return_type = std::max(left->return_type, right->return_type);
	switch (type) {
	case ExpressionType::OPERATOR_ADD:
		ExpressionStatistics::Add(left->stats, right->stats, stats);
		break;
	case ExpressionType::OPERATOR_SUBTRACT:
		ExpressionStatistics::Subtract(left->stats, right->stats, stats);
		break;
	case ExpressionType::OPERATOR_MULTIPLY:
		ExpressionStatistics::Multiply(left->stats, right->stats, stats);
		break;
	case ExpressionType::OPERATOR_DIVIDE:
		ExpressionStatistics::Divide(left->stats, right->stats, stats);
		break;
	case ExpressionType::OPERATOR_MOD:
		ExpressionStatistics::Modulo(left->stats, right->stats, stats);
		break;
	default:
		throw NotImplementedException("Unsupported operator type for statistics!");
	}
	// return the highest type of the children, unless we need to upcast to
	// avoid overflow
	if (!stats.FitsInType(return_type)) {
		return_type = stats.MinimalType();
	}
}

unique_ptr<Expression> OperatorExpression::Copy() {
	auto copy = make_unique<OperatorExpression>(type, return_type);
	copy->CopyProperties(*this);
	copy->left = left->Copy();
	copy->right = right->Copy();
	return copy;
}

void OperatorExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.WriteOptional(left);
	serializer.WriteOptional(right);
}

unique_ptr<Expression> OperatorExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto expression = make_unique<OperatorExpression>(type, return_type);
	expression->left = source.ReadOptional<Expression>();
	expression->right = source.ReadOptional<Expression>();
	return expression;
}

void OperatorExpression::EnumerateChildren(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) {
	if (left) {
		left = callback(move(left));
	}
	if (right) {
		right = callback(move(right));
	}
}

void OperatorExpression::EnumerateChildren(std::function<void(Expression* expression)> callback) const {
	if (left) {
		callback(left.get());
	}
	if (right) {
		callback(right.get());
	}
}


