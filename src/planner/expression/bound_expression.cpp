#include "duckdb/parser/expression/bound_expression.hpp"

namespace duckdb {

BoundExpression::BoundExpression(unique_ptr<Expression> expr)
    : ParsedExpression(ExpressionType::INVALID, ExpressionClass::BOUND_EXPRESSION), expr(std::move(expr)) {
}

string BoundExpression::ToString() const {
	if (!expr) {
		throw InternalException("ToString(): BoundExpression does not have a child");
	}
	return expr->ToString();
}

bool BoundExpression::Equals(const BaseExpression *other) const {
	return false;
}
hash_t BoundExpression::Hash() const {
	return 0;
}

unique_ptr<ParsedExpression> BoundExpression::Copy() const {
	throw SerializationException("Cannot copy or serialize bound expression");
}

void BoundExpression::Serialize(FieldWriter &writer) const {
	throw SerializationException("Cannot copy or serialize bound expression");
}

} // namespace duckdb
