#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/function/scalar/comparison_functions.hpp"

namespace duckdb {

BoundBetweenExpression::BoundBetweenExpression(unique_ptr<Expression> input, unique_ptr<Expression> lower,
                                               unique_ptr<Expression> upper, bool lower_inclusive, bool upper_inclusive)
    : BoundFunctionExpression(BoundScalarFunction(BetweenFun::GetFunction()), {}, nullptr, false),
      lower_inclusive(lower_inclusive), upper_inclusive(upper_inclusive) {
	children.push_back(std::move(input));
	children.push_back(std::move(lower));
	children.push_back(std::move(upper));
	SetExpressionTypeUnsafe(ExpressionType::COMPARE_BETWEEN);
	SetExpressionClassUnsafe(ExpressionClass::BOUND_BETWEEN);
}

string BoundBetweenExpression::ToString() const {
	return BetweenExpression::ToString<BoundBetweenExpression, Expression>(*this);
}

bool BoundBetweenExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) { // NOLINT: explicitly skip BoundFunctionExpression equality
		return false;
	}
	auto &other = other_p.Cast<BoundBetweenExpression>();
	if (!Expression::Equals(Input(), other.Input())) {
		return false;
	}
	if (!Expression::Equals(LowerBound(), other.LowerBound())) {
		return false;
	}
	if (!Expression::Equals(UpperBound(), other.UpperBound())) {
		return false;
	}
	return lower_inclusive == other.lower_inclusive && upper_inclusive == other.upper_inclusive;
}

unique_ptr<Expression> BoundBetweenExpression::Copy() const {
	auto copy = make_uniq<BoundBetweenExpression>(Input().Copy(), LowerBound().Copy(), UpperBound().Copy(),
	                                              lower_inclusive, upper_inclusive);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
