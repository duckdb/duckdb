#include "duckdb/planner/expression/bound_cast_expression.hpp"

using namespace duckdb;
using namespace std;

BoundCastExpression::BoundCastExpression(TypeId target, unique_ptr<Expression> child, SQLType source_type,
                                         SQLType target_type)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, target), child(move(child)),
      source_type(source_type), target_type(target_type) {
}

string BoundCastExpression::ToString() const {
	return "CAST[" + TypeIdToString(return_type) + "](" + child->GetName() + ")";
}

bool BoundCastExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundCastExpression *)other_;
	if (!Expression::Equals(child.get(), other->child.get())) {
		return false;
	}
	if (source_type != other->source_type || target_type != other->target_type) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCastExpression::Copy() {
	auto copy = make_unique<BoundCastExpression>(return_type, child->Copy(), source_type, target_type);
	copy->CopyProperties(*this);
	return move(copy);
}
