#include "parser/expression/common_subexpression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> CommonSubExpression::Copy() const {
	throw SerializationException("CSEs cannot be copied");
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

size_t CommonSubExpression::ChildCount() const {
	return owned_child ? 1 : 0;
}

Expression *CommonSubExpression::GetChild(size_t index) const {
	assert(index == 0 && owned_child);
	return owned_child.get();
}

void CommonSubExpression::ReplaceChild(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback, size_t index) {
	assert(index == 0 && owned_child);
	owned_child = callback(move(owned_child));
}
