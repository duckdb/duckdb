#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression_util.hpp"

namespace duckdb {

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type)
    : Expression(type, ExpressionClass::BOUND_CONJUNCTION, LogicalType::BOOLEAN) {
}

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type, unique_ptr<Expression> left,
                                                       unique_ptr<Expression> right)
    : BoundConjunctionExpression(type) {
	children.push_back(std::move(left));
	children.push_back(std::move(right));
}

string BoundConjunctionExpression::ToString() const {
	return ConjunctionExpression::ToString<BoundConjunctionExpression, Expression>(*this);
}

bool BoundConjunctionExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundConjunctionExpression>();
	return ExpressionUtil::SetEquals(children, other.children);
}

bool BoundConjunctionExpression::PropagatesNullValues() const {
	return false;
}

unique_ptr<Expression> BoundConjunctionExpression::Copy() {
	auto copy = make_uniq<BoundConjunctionExpression>(type);
	for (auto &expr : children) {
		copy->children.push_back(expr->Copy());
	}
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
