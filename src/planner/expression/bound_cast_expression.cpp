#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_default_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

using namespace duckdb;
using namespace std;

BoundCastExpression::BoundCastExpression(TypeId target, unique_ptr<Expression> child, SQLType source_type,
                                         SQLType target_type)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, target), child(move(child)),
      source_type(source_type), target_type(target_type) {
}

unique_ptr<Expression> BoundCastExpression::AddCastToType(unique_ptr<Expression> expr, SQLType source_type,
                                                          SQLType target_type) {
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

bool BoundCastExpression::CastIsInvertible(SQLType source_type, SQLType target_type) {
	if (source_type.id == SQLTypeId::BOOLEAN || target_type.id == SQLTypeId::BOOLEAN) {
		return false;
	}
	if (source_type.id == SQLTypeId::FLOAT || target_type.id == SQLTypeId::FLOAT) {
		return false;
	}
	if (source_type.id == SQLTypeId::DOUBLE || target_type.id == SQLTypeId::DOUBLE) {
		return false;
	}
	if (source_type.id == SQLTypeId::VARCHAR) {
		return target_type.id == SQLTypeId::DATE || target_type.id == SQLTypeId::TIMESTAMP;
	}
	if (target_type.id == SQLTypeId::VARCHAR) {
		return source_type.id == SQLTypeId::DATE || source_type.id == SQLTypeId::TIMESTAMP;
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
