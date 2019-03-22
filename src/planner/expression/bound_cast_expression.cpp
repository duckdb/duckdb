#include "planner/expression/bound_cast_expression.hpp"

using namespace duckdb;
using namespace std;

BoundCastExpression::BoundCastExpression(TypeId target, SQLType sql_type, unique_ptr<Expression> child)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, target, sql_type), child(move(child)) {
}

string BoundCastExpression::ToString() const {
	return "CAST[" + TypeIdToString(return_type) + "](" + child->ToString() + ")";
}

bool BoundCastExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundCastExpression *)other_;
	if (!Expression::Equals(child.get(), other->child.get())) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCastExpression::Copy() {
	auto copy = make_unique<BoundCastExpression>(return_type, sql_type, child->Copy());
	copy->CopyProperties(*this);
	return move(copy);
}
