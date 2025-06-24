#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

BoundExpression::BoundExpression(unique_ptr<Expression> expr_p)
    : ParsedExpression(ExpressionType::INVALID, ExpressionClass::BOUND_EXPRESSION), expr(std::move(expr_p)) {
	this->alias = expr->GetAlias();
}

unique_ptr<Expression> &BoundExpression::GetExpression(ParsedExpression &expr) {
	auto &bound_expr = expr.Cast<BoundExpression>();
	if (!bound_expr.expr) {
		throw InternalException("BoundExpression::GetExpression called on empty bound expression");
	}
	return bound_expr.expr;
}

string BoundExpression::ToString() const {
	if (!expr) {
		throw InternalException("ToString(): BoundExpression does not have a child");
	}
	return expr->ToString();
}

bool BoundExpression::Equals(const BaseExpression &other) const {
	return false;
}
hash_t BoundExpression::Hash() const {
	return 0;
}

unique_ptr<ParsedExpression> BoundExpression::Copy() const {
	throw SerializationException("Cannot copy bound expression");
}

void BoundExpression::Serialize(Serializer &serializer) const {
	throw SerializationException("Cannot serialize bound expression");
}

} // namespace duckdb
