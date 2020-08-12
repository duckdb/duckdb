#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {
using namespace std;

BoundCastExpression::BoundCastExpression(TypeId target, unique_ptr<Expression> child, LogicalType source_type,
                                         LogicalType target_type)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, target), child(move(child)),
      source_type(source_type), target_type(target_type) {
}

unique_ptr<Expression> BoundCastExpression::AddCastToType(unique_ptr<Expression> expr, LogicalType source_type,
                                                          LogicalType target_type) {
	assert(expr);
	if (expr->expression_class == ExpressionClass::BOUND_PARAMETER) {
		auto &parameter = (BoundParameterExpression &)*expr;
		parameter.sql_type = target_type;
		parameter.return_type = GetInternalType(target_type);
	} else if (expr->expression_class == ExpressionClass::BOUND_DEFAULT) {
		auto &def = (BoundDefaultExpression &)*expr;
		def.sql_type = target_type;
		def.return_type = GetInternalType(target_type);
	} else if (source_type != target_type) {
		return make_unique<BoundCastExpression>(GetInternalType(target_type), move(expr), source_type, target_type);
	}
	return expr;
}

bool BoundCastExpression::CastIsInvertible(LogicalType source_type, LogicalType target_type) {
	if (source_type.id == LogicalTypeId::BOOLEAN || target_type.id == LogicalTypeId::BOOLEAN) {
		return false;
	}
	if (source_type.id == LogicalTypeId::FLOAT || target_type.id == LogicalTypeId::FLOAT) {
		return false;
	}
	if (source_type.id == LogicalTypeId::DOUBLE || target_type.id == LogicalTypeId::DOUBLE) {
		return false;
	}
	if (source_type.id == LogicalTypeId::VARCHAR) {
		return target_type.id == LogicalTypeId::DATE || target_type.id == LogicalTypeId::TIMESTAMP;
	}
	if (target_type.id == LogicalTypeId::VARCHAR) {
		return source_type.id == LogicalTypeId::DATE || source_type.id == LogicalTypeId::TIMESTAMP;
	}
	return true;
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

} // namespace duckdb
