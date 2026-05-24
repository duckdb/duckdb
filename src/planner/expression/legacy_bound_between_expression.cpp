#include "duckdb/planner/expression/legacy_bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"

namespace duckdb {

LegacyBoundBetweenExpression::LegacyBoundBetweenExpression(unique_ptr<Expression> input, unique_ptr<Expression> lower,
                                                           unique_ptr<Expression> upper, bool lower_inclusive,
                                                           bool upper_inclusive)
    : Expression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::LEGACY_BOUND_BETWEEN, LogicalType::BOOLEAN),
      input(std::move(input)), lower(std::move(lower)), upper(std::move(upper)), lower_inclusive(lower_inclusive),
      upper_inclusive(upper_inclusive) {
}

unique_ptr<Expression> LegacyBoundBetweenExpression::DeserializeLegacyExpression(unique_ptr<Expression> input,
                                                                                 unique_ptr<Expression> lower,
                                                                                 unique_ptr<Expression> upper,
                                                                                 bool lower_inclusive,
                                                                                 bool upper_inclusive) {
	return BoundBetweenExpression::Create(std::move(input), std::move(lower), std::move(upper), lower_inclusive,
	                                      upper_inclusive);
}

string LegacyBoundBetweenExpression::ToString() const {
	throw InternalException("LegacyBoundBetweenExpression has been deprecated and should no longer be used");
}

unique_ptr<Expression> LegacyBoundBetweenExpression::Copy() const {
	throw InternalException("LegacyBoundBetweenExpression has been deprecated and should no longer be used");
}

} // namespace duckdb
