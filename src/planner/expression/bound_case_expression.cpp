#include "duckdb/planner/expression/bound_case_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

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
	return CaseExpression::ToString<BoundCaseExpression, Expression>(*this);
}

bool BoundCaseExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundCaseExpression>();
	if (!Expression::Equals(case_expr, other.case_expr)) {
		return false;
	}
	if (case_checks.size() != other.case_checks.size()) {
		return false;
	}
	for (idx_t i = 0; i < case_checks.size(); i++) {
		if (!Expression::Equals(*case_checks[i].when_expr, *other.case_checks[i].when_expr)) {
			return false;
		}
		if (!Expression::Equals(*case_checks[i].then_expr, *other.case_checks[i].then_expr)) {
			return false;
		}
		if (!Expression::Equals(case_checks[i].compare_expr, other.case_checks[i].compare_expr)) {
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
	if (case_expr) {
		new_case->case_expr = case_expr->Copy();
	}
	for (auto &check : case_checks) {
		BoundCaseCheck new_check;
		new_check.when_expr = check.when_expr->Copy();
		new_check.then_expr = check.then_expr->Copy();
		if (check.compare_expr) {
			new_check.compare_expr = check.compare_expr->Copy();
		}
		new_case->case_checks.push_back(std::move(new_check));
	}
	new_case->else_expr = else_expr->Copy();

	new_case->CopyProperties(*this);
	return std::move(new_case);
}

bool BoundCaseExpression::CanThrow() const {
	if (Expression::CanThrow()) {
		return true;
	}
	for (auto &check : case_checks) {
		if (check.compare_expr && check.compare_expr->CanThrow()) {
			return true;
		}
	}
	return false;
}

static BoundCaseCheck CopyCaseCheck(const BoundCaseCheck &check) {
	BoundCaseCheck result;
	result.when_expr = check.when_expr->Copy();
	result.then_expr = check.then_expr->Copy();
	if (check.compare_expr) {
		result.compare_expr = check.compare_expr->Copy();
	}
	return result;
}

static unique_ptr<Expression> CreateLegacyCaseComparison(const Expression &case_expr, const BoundCaseCheck &check) {
	D_ASSERT(check.compare_expr);
	auto comparison = check.compare_expr->Copy();
	ExpressionIterator::VisitExpressionMutable<BoundReferenceExpression>(
	    comparison, [&](BoundReferenceExpression &ref, unique_ptr<Expression> &expr) {
		    switch (ref.Index()) {
		    case 0:
			    expr = case_expr.Copy();
			    break;
		    case 1:
			    expr = check.when_expr->Copy();
			    break;
		    default:
			    throw InternalException("Unexpected simple CASE comparison reference index");
		    }
	    });
	return comparison;
}

vector<BoundCaseCheck> BoundCaseExpression::CaseChecksForSerialization(Serializer &serializer) const {
	vector<BoundCaseCheck> result;
	result.reserve(case_checks.size());
	if (!case_expr || serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
		for (auto &check : case_checks) {
			result.push_back(CopyCaseCheck(check));
		}
		return result;
	}

	for (auto &check : case_checks) {
		BoundCaseCheck legacy_check;
		legacy_check.when_expr = CreateLegacyCaseComparison(*case_expr, check);
		legacy_check.then_expr = check.then_expr->Copy();
		result.push_back(std::move(legacy_check));
	}
	return result;
}

} // namespace duckdb
