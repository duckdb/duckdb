#include "parser/expression/operator_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "parser/expression/cast_expression.hpp"

using namespace duckdb;
using namespace std;

void OperatorExpression::ResolveType() {
	Expression::ResolveType();
	if (type == ExpressionType::OPERATOR_NOT) {
		// NOT expression, cast child to BOOLEAN
		assert(children.size() == 1);
		children[0] = CastExpression::AddCastToType(TypeId::BOOLEAN, move(children[0]));
		return_type = TypeId::BOOLEAN;
		return;
	}
	// logical operators return a bool
	if (type == ExpressionType::OPERATOR_IS_NULL || type == ExpressionType::OPERATOR_IS_NOT_NULL) {
		return_type = TypeId::BOOLEAN;
		return;
	}
	if (type == ExpressionType::COMPARE_IN || type == ExpressionType::COMPARE_NOT_IN) {
		return_type = TypeId::BOOLEAN;
		// get the maximum type from the children
		// cast all children to the same type
		auto max_type = children[0]->return_type;
		for(size_t i = 1; i < children.size(); i++) {
			max_type = std::max(max_type, children[i]->return_type);
		}
		for(size_t i = 0; i < children.size(); i++) {
			children[i] = CastExpression::AddCastToType(max_type, move(children[i]));
		}
		return;
	}
	assert(children.size() == 2);
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
	// cast the children to this return type
	for (size_t i = 0; i < children.size(); i++) {
		children[i] = CastExpression::AddCastToType(return_type, move(children[i]));
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
		if (i + 1 < children.size()) {
			result += ", ";
		} else {
			result += ")";
		}
	}
	return result;
}

size_t OperatorExpression::ChildCount() const {
	return children.size();
}

Expression *OperatorExpression::GetChild(size_t index) const {
	assert(index < children.size());
	return children[index].get();
}

void OperatorExpression::ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
                                      size_t index) {
	assert(index < children.size());
	children[index] = callback(move(children[index]));
}
