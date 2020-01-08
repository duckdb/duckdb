#include "duckdb/planner/expression/common_subexpression.hpp"

#include "duckdb/common/exception.hpp"

using namespace duckdb;
using namespace std;

CommonSubExpression::CommonSubExpression(unique_ptr<Expression> child, string alias)
    : Expression(ExpressionType::COMMON_SUBEXPRESSION, ExpressionClass::COMMON_SUBEXPRESSION, child->return_type) {
	this->child = child.get();
	this->owned_child = move(child);
	this->alias = alias;
	assert(this->child);
}

CommonSubExpression::CommonSubExpression(Expression *child, string alias)
    : Expression(ExpressionType::COMMON_SUBEXPRESSION, ExpressionClass::COMMON_SUBEXPRESSION, child->return_type),
      child(child) {
	this->alias = alias;
	assert(child);
}

string CommonSubExpression::ToString() const {
	return child->ToString();
}

bool CommonSubExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (CommonSubExpression *)other_;
	return other->child == child;
}

unique_ptr<Expression> CommonSubExpression::Copy() {
	throw SerializationException("CSEs cannot be copied");
}
