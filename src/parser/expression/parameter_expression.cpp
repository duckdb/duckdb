#include "parser/expression/parameter_expression.hpp"

#include "common/exception.hpp"
#include "common/types/hash.hpp"
#include "common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ParameterExpression::Copy() {
	auto copy = make_unique<ParameterExpression>();
	copy->CopyProperties(*this);
	return move(copy);
}

void ParameterExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
}

unique_ptr<Expression> ParameterExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto expression = make_unique_base<Expression, ParameterExpression>();
	return expression;
}

void ParameterExpression::ResolveType() {
	// Expression::ResolveType();
	// stats.SetFromValue(value);
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
