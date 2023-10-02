#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

BoundLambdaExpression::BoundLambdaExpression(ExpressionType type_p, LogicalType return_type_p,
                                             unique_ptr<Expression> lambda_expr_p, idx_t parameter_count_p)
    : Expression(type_p, ExpressionClass::BOUND_LAMBDA, std::move(return_type_p)),
      lambda_expr(std::move(lambda_expr_p)), parameter_count(parameter_count_p) {
}

string BoundLambdaExpression::ToString() const {
	return lambda_expr->ToString();
}

bool BoundLambdaExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundLambdaExpression>();
	if (!Expression::Equals(*lambda_expr, *other.lambda_expr)) {
		return false;
	}
	if (!Expression::ListEquals(captures, other.captures)) {
		return false;
	}
	if (parameter_count != other.parameter_count) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundLambdaExpression::Copy() {
	auto copy = make_uniq<BoundLambdaExpression>(type, return_type, lambda_expr->Copy(), parameter_count);
	for (auto &capture : captures) {
		copy->captures.push_back(capture->Copy());
	}
	return std::move(copy);
}

} // namespace duckdb
