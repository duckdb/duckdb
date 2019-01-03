#include "parser/expression/cast_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> CastExpression::Copy() {
	auto copy = make_unique<CastExpression>(return_type, child->Copy());
	copy->CopyProperties(*this);
	return copy;
}

void CastExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	child->Serialize(serializer);
}

unique_ptr<Expression> CastExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto child = Expression::Deserialize(source);
	return make_unique_base<Expression, CastExpression>(return_type, move(child));
}

void CastExpression::EnumerateChildren(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) {
	child = callback(move(child));
}

void CastExpression::EnumerateChildren(std::function<void(Expression* expression)> callback) const {
	callback(child.get());
}

