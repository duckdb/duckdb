#include "duckdb/planner/expression/bound_filter_expression.hpp"

namespace duckdb {

BoundFilterExpression::BoundFilterExpression(unique_ptr<Expression> filter)
    : Expression(ExpressionType::FILTER_EXPR, ExpressionClass::BOUND_FILTER, filter->return_type), filter(move(filter)) {
}

string BoundFilterExpression::ToString() const {
	return filter->GetName();
}

bool BoundFilterExpression::Equals(const BaseExpression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (BoundFilterExpression *)other_;
	if (!Expression::Equals(filter.get(), other->filter.get())) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundFilterExpression::Copy() {
	auto new_case = make_unique<BoundFilterExpression>(filter->Copy());
	new_case->CopyProperties(*this);
	return move(new_case);
}

} // namespace duckdb
