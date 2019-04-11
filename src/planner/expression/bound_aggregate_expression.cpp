#include "planner/expression/bound_aggregate_expression.hpp"

using namespace duckdb;
using namespace std;

BoundAggregateExpression::BoundAggregateExpression(TypeId return_type, ExpressionType type,
                                                   unique_ptr<Expression> child)
    : Expression(type, ExpressionClass::BOUND_AGGREGATE, return_type), child(move(child)) {
}

string BoundAggregateExpression::ToString() const {
	return ExpressionTypeToString(type) + "(" + (child ? child->GetName() : string()) + ")";
}

bool BoundAggregateExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundAggregateExpression *)other_;
	return Expression::Equals(child.get(), other->child.get());
}

unique_ptr<Expression> BoundAggregateExpression::Copy() {
	auto new_child = child ? child->Copy() : nullptr;
	auto new_aggregate = make_unique<BoundAggregateExpression>(return_type, type, move(new_child));
	new_aggregate->CopyProperties(*this);
	return move(new_aggregate);
}
