#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/planner/expression/legacy_bound_comparison_expression.hpp"

namespace duckdb {

LegacyBoundComparisonExpression::LegacyBoundComparisonExpression(ExpressionType type, unique_ptr<Expression> left,
                                                                 unique_ptr<Expression> right)
    : Expression(type, ExpressionClass::LEGACY_BOUND_COMPARISON, LogicalType::BOOLEAN), left(std::move(left)),
      right(std::move(right)) {
}

string LegacyBoundComparisonExpression::ToString() const {
	throw InternalException("LegacyBoundComparisonExpression has been deprecated and should no longer be used");
}

bool LegacyBoundComparisonExpression::Equals(const BaseExpression &other_p) const {
	throw InternalException("LegacyBoundComparisonExpression has been deprecated and should no longer be used");
}

unique_ptr<Expression> LegacyBoundComparisonExpression::Copy() const {
	throw InternalException("LegacyBoundComparisonExpression has been deprecated and should no longer be used");
}

unique_ptr<Expression> LegacyBoundComparisonExpression::DeserializeLegacyExpression(ExpressionType type,
                                                                                    unique_ptr<Expression> left,
                                                                                    unique_ptr<Expression> right) {
	return BoundComparisonExpression::Create(type, std::move(left), std::move(right));
}

} // namespace duckdb
