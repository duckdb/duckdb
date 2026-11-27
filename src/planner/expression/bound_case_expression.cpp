#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"

namespace duckdb {

BoundCaseExpression::BoundCaseExpression(LogicalType type)
    : Expression(ExpressionType::CASE_EXPR, ExpressionClass::BOUND_CASE, std::move(type)) {
}

BoundCaseExpression::BoundCaseExpression(unique_ptr<Expression> when_expr, unique_ptr<Expression> then_expr,
                                         unique_ptr<Expression> else_expr_p)
    : Expression(ExpressionType::CASE_EXPR, ExpressionClass::BOUND_CASE, then_expr->GetReturnType()),
      else_expr(std::move(else_expr_p)) {
	BoundCaseCheck check;
	check.when_expr = std::move(when_expr);
	check.then_expr = std::move(then_expr);
	case_checks.push_back(std::move(check));
}

string BoundCaseExpression::ToString() const {
	auto checks = CaseExpression::ToString<BoundCaseExpression, Expression>(*this);
	if (operand) {
		return "CASE OPERAND (" + operand->ToString() + ") " + checks;
	}
	return checks;
}

void BoundCaseExpression::LegacySerialize(Serializer &) const {
	throw SerializationException("A bound plan with a shared CASE operand requires storage version v2.1.0 or newer");
}

bool BoundCaseExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundCaseExpression>();
	if (!Expression::Equals(operand, other.operand) || case_checks.size() != other.case_checks.size()) {
		return false;
	}
	for (idx_t i = 0; i < case_checks.size(); i++) {
		if (!Expression::Equals(*case_checks[i].when_expr, *other.case_checks[i].when_expr)) {
			return false;
		}
		if (!Expression::Equals(*case_checks[i].then_expr, *other.case_checks[i].then_expr)) {
			return false;
		}
	}
	if (!Expression::Equals(*else_expr, *other.else_expr)) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCaseExpression::Copy() const {
	auto new_case = make_uniq<BoundCaseExpression>(return_type);
	for (auto &check : case_checks) {
		BoundCaseCheck new_check;
		new_check.when_expr = check.when_expr->Copy();
		new_check.then_expr = check.then_expr->Copy();
		new_case->case_checks.push_back(std::move(new_check));
	}
	new_case->operand = operand ? operand->Copy() : nullptr;
	new_case->else_expr = else_expr->Copy();

	new_case->CopyProperties(*this);
	return std::move(new_case);
}

} // namespace duckdb
