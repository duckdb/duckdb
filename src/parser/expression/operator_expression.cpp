#include "parser/expression/operator_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

OperatorExpression::OperatorExpression(ExpressionType type,
                    unique_ptr<ParsedExpression> left,
                    unique_ptr<ParsedExpression> right)
	: ParsedExpression(type, ExpressionClass::OPERATOR) {
	if (left) {
		children.push_back(move(left));
	}
	if (right) {
		children.push_back(move(right));
	}
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

bool OperatorExpression::Equals(const ParsedExpression *other_) const {
	if (!ParsedExpression::Equals(other_)) {
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

unique_ptr<ParsedExpression> OperatorExpression::Copy() {
	auto copy = make_unique<OperatorExpression>(type);
	copy->CopyProperties(*this);
	for (auto &it : children) {
		copy->children.push_back(it->Copy());
	}
	return move(copy);
}

void OperatorExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteList(children);
}

unique_ptr<ParsedExpression> OperatorExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto expression = make_unique<OperatorExpression>(type);
	source.ReadList<ParsedExpression>(expression->children);
	return move(expression);
}

size_t OperatorExpression::ChildCount() const {
	return children.size();
}

ParsedExpression *OperatorExpression::GetChild(size_t index) const {
	assert(index < children.size());
	return children[index].get();
}

void OperatorExpression::ReplaceChild(std::function<unique_ptr<ParsedExpression>(unique_ptr<ParsedExpression> expression)> callback,
                                      size_t index) {
	assert(index < children.size());
	children[index] = callback(move(children[index]));
}
