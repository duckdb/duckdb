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
	return_type = std::max(children[0]->return_type, children[1]->return_type);
	switch (type) {
	case ExpressionType::OPERATOR_ADD:
		ExpressionStatistics::Add(children[0]->stats, children[1]->stats, stats);
		break;
	case ExpressionType::OPERATOR_SUBTRACT:
		ExpressionStatistics::Subtract(children[0]->stats, children[1]->stats, stats);
		break;
	case ExpressionType::OPERATOR_MULTIPLY:
		ExpressionStatistics::Multiply(children[0]->stats, children[1]->stats, stats);
		break;
	case ExpressionType::OPERATOR_DIVIDE:
		ExpressionStatistics::Divide(children[0]->stats, children[1]->stats, stats);
		break;
	case ExpressionType::OPERATOR_MOD:
		ExpressionStatistics::Modulo(children[0]->stats, children[1]->stats, stats);
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
	for (auto &it : children) {
		copy->children.push_back(it->Copy());
	}
	return copy;
}

void OperatorExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.WriteList(children);
}

unique_ptr<Expression> OperatorExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto expression = make_unique<OperatorExpression>(type, return_type);
	source.ReadList<Expression>(expression->children);
	return expression;
}

bool OperatorExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (OperatorExpression *)other_;
	if (children.size() != other->children.size()) {
		return false;
	}
	for (size_t i = 0; i < children.size(); i++) {
		if (!children[i]->Equals(other->children[i].get())) {
			return false;
		}
	}
	return true;
}

string OperatorExpression::ToString() const {
	auto op = ExpressionTypeToOperator(type);
	if (!op.empty()) {
		// use the operator string to represent the operator
		if (children.size() == 1) {
			return op + children[0]->ToString();
		} else if (children.size() == 2) {
			return children[0]->ToString() + " " + op + " " + children[1]->ToString();
		}
	}
	// if there is no operator we render it as a function
	auto result = ExpressionTypeToString(type) + "(";
	for (size_t i = 0; i < children.size(); i++) {
		result += children[i]->ToString();
		if (i + 1 == children.size()) {
			result += ", ";
		} else {
			result += ")";
		}
	}
	return result;
}

void OperatorExpression::EnumerateChildren(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) {
	for (size_t i = 0; i < children.size(); i++) {
		children[i] = callback(move(children[i]));
	}
}

void OperatorExpression::EnumerateChildren(std::function<void(Expression *expression)> callback) const {
	for (size_t i = 0; i < children.size(); i++) {
		callback(children[i].get());
	}
}
