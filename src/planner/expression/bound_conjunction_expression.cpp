#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parser/expression_util.hpp"

using namespace duckdb;
using namespace std;

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type)
    : Expression(type, ExpressionClass::BOUND_CONJUNCTION, TypeId::BOOL) {
}

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type, unique_ptr<Expression> left,
                                                       unique_ptr<Expression> right)
    : Expression(type, ExpressionClass::BOUND_CONJUNCTION, TypeId::BOOL) {
	children.push_back(move(left));
	children.push_back(move(right));
}

string BoundConjunctionExpression::ToString() const {
	string result = "(" + children[0]->ToString();
	for (idx_t i = 1; i < children.size(); i++) {
		result += " " + ExpressionTypeToOperator(type) + " " + children[i]->ToString();
	}
	return result + ")";
}

bool BoundConjunctionExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundConjunctionExpression *)other_;
	return ExpressionUtil::SetEquals(children, other->children);
}

unique_ptr<Expression> BoundConjunctionExpression::Copy() {
	auto copy = make_unique<BoundConjunctionExpression>(type);
	for (auto &expr : children) {
		copy->children.push_back(expr->Copy());
	}
	copy->CopyProperties(*this);
	return move(copy);
}
