#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {
using namespace std;

BoundCastExpression::BoundCastExpression(unique_ptr<Expression> child, LogicalType target_type)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, move(target_type)), child(move(child)) {
}

unique_ptr<Expression> BoundCastExpression::AddCastToType(unique_ptr<Expression> expr, LogicalType target_type) {
	assert(expr);
	if (expr->expression_class == ExpressionClass::BOUND_PARAMETER) {
		auto &parameter = (BoundParameterExpression &)*expr;
		parameter.return_type = target_type;
	} else if (expr->expression_class == ExpressionClass::BOUND_DEFAULT) {
		auto &def = (BoundDefaultExpression &)*expr;
		def.return_type = target_type;
	} else if (expr->return_type != target_type) {
		return make_unique<BoundCastExpression>(move(expr), target_type);
	}
	return expr;
}

bool BoundCastExpression::CastIsInvertible(LogicalType source_type, LogicalType target_type) {
	if (source_type.id() == LogicalTypeId::BOOLEAN || target_type.id() == LogicalTypeId::BOOLEAN) {
		return false;
	}
	if (source_type.id() == LogicalTypeId::FLOAT || target_type.id() == LogicalTypeId::FLOAT) {
		return false;
	}
	if (source_type.id() == LogicalTypeId::DOUBLE || target_type.id() == LogicalTypeId::DOUBLE) {
		return false;
	}
	if (source_type.id() == LogicalTypeId::VARCHAR) {
		return target_type.id() == LogicalTypeId::DATE || target_type.id() == LogicalTypeId::TIMESTAMP;
	}
	if (target_type.id() == LogicalTypeId::VARCHAR) {
		return source_type.id() == LogicalTypeId::DATE || source_type.id() == LogicalTypeId::TIMESTAMP;
	}
	return true;
}

string BoundCastExpression::ToString() const {
	return "CAST[" + return_type.ToString() + "](" + child->GetName() + ")";
}

bool BoundCastExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundCastExpression *)other_;
	if (!Expression::Equals(child.get(), other->child.get())) {
		return false;
	}
	if (return_type != other->return_type) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCastExpression::Copy() {
	auto copy = make_unique<BoundCastExpression>(child->Copy(), return_type);
	copy->CopyProperties(*this);
	return move(copy);
}

} // namespace duckdb
