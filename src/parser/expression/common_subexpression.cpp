#include "parser/expression/common_subexpression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> CommonSubExpression::Copy() {
	throw SerializationException("CSEs cannot be copied");
}

void CommonSubExpression::EnumerateChildren(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) {
	if (owned_child) {
		owned_child = callback(move(owned_child));
	}
}
void CommonSubExpression::EnumerateChildren(std::function<void(Expression *expression)> callback) const {
	if (owned_child) {
		owned_child.get();
	}
}

void CommonSubExpression::Serialize(Serializer &serializer) {
	throw SerializationException("CSEs cannot be serialized");
}

bool CommonSubExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (CommonSubExpression *)other_;
	return other->child == child;
}
