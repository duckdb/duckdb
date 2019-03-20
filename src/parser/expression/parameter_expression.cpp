#include "parser/expression/parameter_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "common/types/hash.hpp"
#include "common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

void ParameterExpression::ResolveType() {
}

unique_ptr<Expression> ParameterExpression::Copy() const {
	auto copy = make_unique<ParameterExpression>();
	copy->CopyProperties(*this);
	copy->value = value;
	return move(copy);
}

void ParameterExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.Write<uint64_t>(parameter_nr);
	value.Serialize(serializer);
}

unique_ptr<Expression> ParameterExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto expression = make_unique<ParameterExpression>();
	expression->parameter_nr = source.Read<uint64_t>();
	expression->value = Value::Deserialize(source);
	return move(expression);
}

bool ParameterExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	return true;
}

uint64_t ParameterExpression::Hash() const {
	return Expression::Hash();
}
