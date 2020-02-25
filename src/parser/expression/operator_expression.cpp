#include "duckdb/parser/expression/operator_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

OperatorExpression::OperatorExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
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
	result += StringUtil::Join(children, children.size(), ", ",
	                           [](const unique_ptr<ParsedExpression> &child) { return child->ToString(); });
	result += ")";
	return result;
}

bool OperatorExpression::Equals(const OperatorExpression *a, const OperatorExpression *b) {
	if (a->children.size() != b->children.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->children.size(); i++) {
		if (!a->children[i]->Equals(b->children[i].get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<ParsedExpression> OperatorExpression::Copy() const {
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
