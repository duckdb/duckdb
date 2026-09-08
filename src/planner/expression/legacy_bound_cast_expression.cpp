#include "duckdb/planner/expression/legacy_bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

LegacyBoundCastExpression::LegacyBoundCastExpression(unique_ptr<Expression> child_p, LogicalType target_type_p,
                                                     bool try_cast_p)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::LEGACY_BOUND_CAST, std::move(target_type_p)),
      child(std::move(child_p)), try_cast(try_cast_p) {
}

string LegacyBoundCastExpression::ToString() const {
	throw InternalException("LegacyBoundCastExpression has been deprecated and should no longer be used");
}

bool LegacyBoundCastExpression::Equals(const BaseExpression &other_p) const {
	throw InternalException("LegacyBoundCastExpression has been deprecated and should no longer be used");
}

unique_ptr<Expression> LegacyBoundCastExpression::Copy() const {
	throw InternalException("LegacyBoundCastExpression has been deprecated and should no longer be used");
}

unique_ptr<Expression> LegacyBoundCastExpression::DeserializeLegacyExpression(ClientContext &context,
                                                                              unique_ptr<Expression> child,
                                                                              const LogicalType &target_type,
                                                                              bool try_cast) {
	return BoundCastExpression::AddCastToType(context, std::move(child), target_type, try_cast);
}

} // namespace duckdb
