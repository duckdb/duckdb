#include "duckdb/planner/expression/bound_between_expression.hpp"

using namespace duckdb;
using namespace std;

BoundBetweenExpression::BoundBetweenExpression(unique_ptr<Expression> input, unique_ptr<Expression> lower,
                                               unique_ptr<Expression> upper, bool lower_inclusive, bool upper_inclusive)
    : Expression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BOUND_BETWEEN, TypeId::BOOL), input(move(input)),
      lower(move(lower)), upper(move(upper)), lower_inclusive(lower_inclusive), upper_inclusive(upper_inclusive) {
}

string BoundBetweenExpression::ToString() const {
	return input->ToString() + " BETWEEN " + lower->ToString() + " AND " + upper->ToString();
}

bool BoundBetweenExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundBetweenExpression *)other_;
	if (!Expression::Equals(input.get(), other->input.get())) {
		return false;
	}
	if (!Expression::Equals(lower.get(), other->lower.get())) {
		return false;
	}
	if (!Expression::Equals(upper.get(), other->upper.get())) {
		return false;
	}
	return lower_inclusive == other->lower_inclusive && upper_inclusive == other->upper_inclusive;
}

unique_ptr<Expression> BoundBetweenExpression::Copy() {
	auto copy = make_unique<BoundBetweenExpression>(input->Copy(), lower->Copy(), upper->Copy(), lower_inclusive,
	                                                upper_inclusive);
	copy->CopyProperties(*this);
	return move(copy);
}
