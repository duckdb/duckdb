#include "duckdb/planner/expression/bound_case_expression.hpp"

using namespace duckdb;
using namespace std;

BoundCaseExpression::BoundCaseExpression(unique_ptr<Expression> check, unique_ptr<Expression> res_if_true,
                                         unique_ptr<Expression> res_if_false)
    : Expression(ExpressionType::CASE_EXPR, ExpressionClass::BOUND_CASE, res_if_true->return_type), check(move(check)),
      result_if_true(move(res_if_true)), result_if_false(move(res_if_false)) {
}

string BoundCaseExpression::ToString() const {
	return "CASE WHEN (" + check->GetName() + ") THEN (" + result_if_true->GetName() + ") ELSE (" +
	       result_if_false->GetName() + ")";
}

bool BoundCaseExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundCaseExpression *)other_;
	if (!Expression::Equals(check.get(), other->check.get())) {
		return false;
	}
	if (!Expression::Equals(result_if_true.get(), other->result_if_true.get())) {
		return false;
	}
	if (!Expression::Equals(result_if_false.get(), other->result_if_false.get())) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCaseExpression::Copy() {
	auto new_case = make_unique<BoundCaseExpression>(check->Copy(), result_if_true->Copy(), result_if_false->Copy());
	new_case->CopyProperties(*this);
	return move(new_case);
}
