#include "duckdb/planner/expression/bound_expanded_expression.hpp"

namespace duckdb {

BoundExpandedExpression::BoundExpandedExpression(vector<unique_ptr<Expression>> expanded_expressions_p)
    : Expression(ExpressionType::BOUND_EXPANDED, ExpressionClass::BOUND_EXPANDED, LogicalType::INTEGER),
      expanded_expressions(std::move(expanded_expressions_p)) {
}

string BoundExpandedExpression::ToString() const {
	return "BOUND_EXPANDED";
}

bool BoundExpandedExpression::Equals(const BaseExpression &other_p) const {
	return false;
}

unique_ptr<Expression> BoundExpandedExpression::Copy() const {
	throw SerializationException("Cannot copy BoundExpandedExpression");
}

} // namespace duckdb
