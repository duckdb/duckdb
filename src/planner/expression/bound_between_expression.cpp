#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"

namespace duckdb {

BoundBetweenExpression::BoundBetweenExpression()
    : Expression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BOUND_BETWEEN, LogicalType::BOOLEAN) {
}

BoundBetweenExpression::BoundBetweenExpression(unique_ptr<Expression> input, unique_ptr<Expression> lower,
                                               unique_ptr<Expression> upper, bool lower_inclusive, bool upper_inclusive)
    : Expression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BOUND_BETWEEN, LogicalType::BOOLEAN),
      input(std::move(input)), lower(std::move(lower)), upper(std::move(upper)), lower_inclusive(lower_inclusive),
      upper_inclusive(upper_inclusive) {
}

string BoundBetweenExpression::ToString() const {
	return BetweenExpression::ToString<BoundBetweenExpression, Expression>(*this);
}

bool BoundBetweenExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundBetweenExpression>();
	if (!Expression::Equals(*input, *other.input)) {
		return false;
	}
	if (!Expression::Equals(*lower, *other.lower)) {
		return false;
	}
	if (!Expression::Equals(*upper, *other.upper)) {
		return false;
	}
	return lower_inclusive == other.lower_inclusive && upper_inclusive == other.upper_inclusive;
}

unique_ptr<Expression> BoundBetweenExpression::Copy() {
	auto copy = make_uniq<BoundBetweenExpression>(input->Copy(), lower->Copy(), upper->Copy(), lower_inclusive,
	                                              upper_inclusive);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
